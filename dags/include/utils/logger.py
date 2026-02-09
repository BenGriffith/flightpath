import logging
import sys
from typing import Optional


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger that's suitable for local development or Airflow."""
    if name is None:
        name = __name__

    logger = logging.getLogger(name)

    # Always ensure INFO level (default might be WARNING or NOTSET)
    logger.setLevel(logging.INFO)

    # Always allow propagation so Airflow's root logger catches the logs
    logger.propagate = True

    # If already configured, we are done
    if getattr(logger, "_configured_by_project", False):
        return logger

    # If we are effectively "local" (no existing handlers), add a stream handler.
    # This ensures functionality for local scripts.
    # We DO NOT disable propagation, so this is safe for Airflow too
    # (Airflow will capture the propagated log + potentially this stdout one).
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    setattr(logger, "_configured_by_project", True)
    return logger
