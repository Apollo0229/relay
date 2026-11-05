import asyncio

import psycopg
import pytest

PAYMENT = {"type": "payment.succeeded", "data": {"amount": 1200, "currency": "usd"}}


async def test_accepts_an_event_and_writes_its_outbox_row(client, db):
    res = await client.post("/v1/events/stripe", json=PAYMENT)
    assert res.status_code == 202
    body = res.json()
    assert body["duplicate"] is False

    (source, type_, payload) = db.execute("select source, type, payload from events").fetchone()
    assert (source, type_, payload) == ("stripe", "payment.succeeded", PAYMENT["data"])

    (topic, key, envelope, published_at) = db.execute(
        "select topic, key, payload, published_at from outbox"
    ).fetchone()
    assert topic == "relay.events"
    assert key == "stripe"
    assert published_at is None
    assert envelope["id"] == body["id"]
    assert envelope["data"] == PAYMENT["data"]
    assert envelope["received_at"]


async def test_same_idempotency_key_returns_the_original_event(client, db):
    headers = {"Idempotency-Key": "evt_123"}
    first = await client.post("/v1/events/stripe", json=PAYMENT, headers=headers)
    second = await client.post("/v1/events/stripe", json=PAYMENT, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 200
    assert second.json() == {"id": first.json()["id"], "duplicate": True}
    assert db.execute("select count(*) from events").fetchone() == (1,)
    assert db.execute("select count(*) from outbox").fetchone() == (1,)


async def test_idempotency_keys_are_scoped_per_source(client, db):
    headers = {"Idempotency-Key": "evt_123"}
    await client.post("/v1/events/stripe", json=PAYMENT, headers=headers)
    res = await client.post("/v1/events/shopify", json=PAYMENT, headers=headers)
    assert res.status_code == 202
    assert db.execute("select count(*) from outbox").fetchone() == (2,)


async def test_events_without_a_key_are_never_deduplicated(client, db):
    for _ in range(3):
        assert (await client.post("/v1/events/stripe", json=PAYMENT)).status_code == 202
    assert db.execute("select count(*) from outbox").fetchone() == (3,)


async def test_concurrent_retries_store_the_event_exactly_once(client, db):
    headers = {"Idempotency-Key": "race"}
    responses = await asyncio.gather(
        *(client.post("/v1/events/stripe", json=PAYMENT, headers=headers) for _ in range(20))
    )

    ids = {r.json()["id"] for r in responses}
    assert len(ids) == 1
    assert sorted(r.status_code for r in responses) == [200] * 19 + [202]
    assert db.execute("select count(*) from events").fetchone() == (1,)
    assert db.execute("select count(*) from outbox").fetchone() == (1,)


async def test_event_and_outbox_row_commit_together(client, db):
    # Without the outbox table, the outbox insert fails; the event insert must roll back with it.
    db.execute("alter table outbox rename to outbox_away")
    try:
        with pytest.raises(psycopg.errors.UndefinedTable):
            await client.post("/v1/events/stripe", json=PAYMENT)
    finally:
        db.execute("alter table outbox_away rename to outbox")
    assert db.execute("select count(*) from events").fetchone() == (0,)


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/v1/events/Stripe!", PAYMENT),
        ("/v1/events/stripe", {"data": {}}),
        ("/v1/events/stripe", {"type": "has spaces", "data": {}}),
        ("/v1/events/stripe", {"type": "ok", "data": [1, 2]}),
    ],
)
async def test_rejects_invalid_requests(client, db, path, body):
    assert (await client.post(path, json=body)).status_code == 422
    assert db.execute("select count(*) from events").fetchone() == (0,)


async def test_stats_and_health(client):
    await client.post("/v1/events/stripe", json=PAYMENT)
    assert (await client.get("/v1/stats")).json() == {"events": 1, "pending": 1, "published": 0}
    assert (await client.get("/healthz")).json() == {"status": "ok"}


async def test_dashboard_follows_events_through_the_pipeline(client, db):
    ids = [
        (await client.post(f"/v1/events/{s}", json=PAYMENT)).json()["id"]
        for s in ("stripe", "stripe", "github")
    ]
    db.execute("update outbox set published_at = now() where event_id = any(%s::uuid[])", (ids[:2],))
    db.execute(
        """
        insert into processed_events (consumer, event_id, status, attempts, error) values
          ('event-counter', %s, 'done', 1, null),
          ('event-counter', %s, 'dead', 3, 'RuntimeError: downstream outage')
        """,
        (ids[0], ids[1]),
    )
    db.execute("insert into event_counts values ('stripe', 'payment.succeeded', 1)")

    body = (await client.get("/v1/dashboard")).json()

    assert {k: body[k] for k in ("events", "pending", "published", "processed", "retried", "dead")} == {
        "events": 3,
        "pending": 1,
        "published": 2,
        "processed": 1,
        "retried": 0,
        "dead": 1,
    }
    assert body["sources"] == [{"source": "stripe", "count": 1}]
    [dead] = body["dead_letters"]
    assert (dead["id"], dead["source"], dead["attempts"]) == (ids[1], "stripe", 3)
    assert dead["error"] == "RuntimeError: downstream outage"


async def test_dashboard_stream_yields_fresh_snapshots(client):
    app = client._transport.app
    stream = next(r for r in app.routes if getattr(r, "path", "") == "/v1/dashboard/stream").endpoint

    class FakeRequest:
        pass

    request = FakeRequest()
    request.app = app
    snapshots = stream(request)
    assert (await anext(snapshots)).events == 0
    await client.post("/v1/events/stripe", json=PAYMENT)
    assert (await anext(snapshots)).events == 1
    await snapshots.aclose()
