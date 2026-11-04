-- Every accepted webhook is stored once, with its outbox row written in the same transaction.

create table if not exists events (
  id              uuid primary key,
  source          text        not null,
  type            text        not null,
  idempotency_key text,
  payload         jsonb       not null,
  received_at     timestamptz not null default now(),
  -- A retried webhook with the same key maps to the original event.
  unique (source, idempotency_key)
);

create table if not exists outbox (
  id           bigserial   primary key,
  event_id     uuid        not null references events (id),
  topic        text        not null,
  key          text        not null,
  payload      jsonb       not null,
  created_at   timestamptz not null default now(),
  published_at timestamptz,
  attempts     int         not null default 0,
  last_error   text
);

-- The relay only ever scans unpublished rows, oldest first.
create index if not exists outbox_pending on outbox (id) where published_at is null;
