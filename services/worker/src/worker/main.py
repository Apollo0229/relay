import logging
import signal
import sys

from psycopg_pool import ConnectionPool

from .consumer import Worker
from .handlers import count_event
from .processing import Processor
from .settings import Settings


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
    settings = Settings()

    with ConnectionPool(settings.database_url, min_size=1, max_size=len(settings.retry_delays) + 2) as pool:
        processor = Processor(
            pool, settings.consumer, count_event, settings.retry_topics, settings.dead_letter_topic
        )
        worker = Worker(settings, processor)
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: worker.stopping.set())

        worker.start()
        logging.info("worker started on %s", ", ".join(t.topic for t in worker.tiers))
        while not worker.stopping.wait(1):  # short waits keep Ctrl+C responsive on Windows
            pass
        logging.info("worker shutting down")
        worker.stop()

    sys.exit(1 if worker.failed.is_set() else 0)


if __name__ == "__main__":
    main()
