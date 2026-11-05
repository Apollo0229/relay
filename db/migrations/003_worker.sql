-- Workers record each event they finish in the same transaction as its effects,
-- so a redelivered Kafka message is recognised and skipped.

create table if not exists processed_events (
  consumer     text        not null,
  event_id     uuid        not null,
  -- done: effects applied. dead: given up on and sent to the dead-letter topic.
  status       text        not null check (status in ('done', 'dead')),
  attempts     int         not null,
  error        text,
  processed_at timestamptz not null default now(),
  primary key (consumer, event_id)
);

-- The demo projection: how many events of each type every source has sent.
-- Counting twice would show up here, which is what idempotency prevents.
create table if not exists event_counts (
  source text   not null,
  type   text   not null,
  count  bigint not null default 0,
  primary key (source, type)
);
