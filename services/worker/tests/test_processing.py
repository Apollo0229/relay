import threading
import uuid

import pytest
from conftest import counts, processed

from worker.handlers import Event, PermanentError, count_event
from worker.processing import Outcome, Processor

RETRY = ["relay.events.retry.1", "relay.events.retry.2"]
DLQ = "relay.events.dlq"


def make_processor(pool, handler=count_event):
    return Processor(pool, "event-counter", handler, RETRY, DLQ)


def event(**data) -> Event:
    return Event(id=uuid.uuid4(), source="stripe", type="payment.succeeded", data=data)


def test_applies_an_event_once_however_often_it_is_delivered(pool):
    p, e = make_processor(pool), event(amount=1200)

    assert p.process(e, 1) == Outcome("done")
    assert p.process(e, 1) == Outcome("duplicate")
    assert p.process(e, 2) == Outcome("duplicate")
    assert counts(pool) == {("stripe", "payment.succeeded"): 1}


def test_a_failure_rolls_back_the_claim_so_the_retry_can_run(pool):
    p, e = make_processor(pool), event(simulate="flaky")

    first = p.process(e, 1)
    assert first.status == "retry" and first.next_topic == RETRY[0]
    assert "first attempt" in first.error
    assert processed(pool) == []

    assert p.process(e, 2) == Outcome("done")
    assert counts(pool) == {("stripe", "payment.succeeded"): 1}
    assert processed(pool) == [("done", 2, None)]


def test_retries_on_each_delay_topic_then_gives_up(pool):
    p, e = make_processor(pool), event(simulate="error")

    assert [p.process(e, n).next_topic for n in (1, 2, 3)] == [RETRY[0], RETRY[1], DLQ]
    assert processed(pool) == [("dead", 3, "RuntimeError: simulated downstream outage")]
    assert counts(pool) == {}


def test_a_permanent_error_skips_the_retries(pool):
    outcome = make_processor(pool).process(event(simulate="invalid"), 1)

    assert outcome == Outcome("dead", DLQ, "PermanentError: event marked invalid")
    assert processed(pool)[0][:2] == ("dead", 1)


def test_a_dead_event_is_not_processed_again(pool):
    p, e = make_processor(pool), event(simulate="invalid")
    p.process(e, 1)

    assert p.process(e, 1) == Outcome("duplicate")


def test_concurrent_deliveries_apply_the_event_once(pool):
    entered = threading.Barrier(8)
    p, e = make_processor(pool), event()
    results: list[Outcome] = []

    def deliver():
        entered.wait()
        results.append(p.process(e, 1))

    threads = [threading.Thread(target=deliver) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(o.status for o in results) == ["done"] + ["duplicate"] * 7
    assert counts(pool) == {("stripe", "payment.succeeded"): 1}


@pytest.mark.parametrize("raw", [b"not json", b"[]", b'{"id": "nope"}', b'{"source": "x"}'])
def test_malformed_events_are_permanent_errors(raw):
    with pytest.raises(PermanentError):
        Event.parse(raw)
