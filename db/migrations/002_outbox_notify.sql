-- Wake the relay as soon as outbox rows are written, instead of waiting for its next poll.
-- Notifications are sent on commit, and repeats within one transaction collapse into one.

create or replace function notify_outbox() returns trigger
language plpgsql as $$
begin
  perform pg_notify('outbox', '');
  return null;
end
$$;

create or replace trigger outbox_notify
  after insert on outbox
  for each statement execute function notify_outbox();
