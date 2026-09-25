"""Gunicorn config for Docker / production-style runs (single worker + collector thread)."""

import os

bind = f"0.0.0.0:{os.getenv('PORT', '8080')}"
workers = 1
threads = int(os.getenv("GUNICORN_THREADS", "8"))
timeout = 120
accesslog = "-"
errorlog = "-"


def post_worker_init(worker) -> None:
    from collect_metrics import start_collector

    initial_run = os.getenv("COLLECTOR_INITIAL_RUN", "1").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    start_collector(initial_run=initial_run)


def worker_exit(server, worker) -> None:
    from collect_metrics import stop_collector

    stop_collector()
