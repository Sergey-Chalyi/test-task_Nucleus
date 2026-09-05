"""Minimal structured-ish logging setup shared by the API and the worker."""

import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger once, writing single-line records to stdout."""
    root = logging.getLogger()
    if root.handlers:  # already configured (e.g. uvicorn reload)
        root.setLevel(level.upper())
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    root.addHandler(handler)
    root.setLevel(level.upper())
