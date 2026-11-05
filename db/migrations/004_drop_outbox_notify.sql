-- Replaces 002. Postgres serialises the commits of every transaction that sends
-- NOTIFY on one global lock, which capped ingest at ~340 events/s under load.
-- The relay now polls the outbox_pending index every 100ms instead, which costs
-- next to nothing when the outbox is empty.

drop trigger if exists outbox_notify on outbox;
drop function if exists notify_outbox();
