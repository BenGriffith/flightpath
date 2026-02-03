import logging
import os
from typing import Optional


def _configure_local_logger(logger: logging.Logger) -> None:
    if getattr(logger, "_configured_by_project", False):
        return

    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    handler.setFormatter(fmt)

    # avoid duplicating handlers
    logger.handlers = []
    logger.addHandler(handler)
    logger.propagate = False

    setattr(logger, "_configured_by_project", True)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger that's suitable for local development or Airflow."""
    if name is None:
        name = __name__

    logger = logging.getLogger(name)

    # Airflow sets AIRFLOW_CTX_DAG_ID in task runtime. Prefer Airflow's
    # logging configuration when present.
    if os.getenv("AIRFLOW_CTX_DAG_ID"):
        return logger

    # local development: ensure a readable console logger
    _configure_local_logger(logger)
    return logger
