# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Structured logging setup (structlog).

Logs are emitted as JSON lines to stdout; they are consumed by the harness for
event-log construction as well as by humans during debugging. The setup is
idempotent — repeated calls are no-ops.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


_CONFIGURED = False


def configure(level: str = "INFO") -> None:
    """Configure structlog + stdlib logging to emit JSON lines to stdout."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper())),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _CONFIGURED = True


def get_logger(name: str, **context: Any) -> structlog.stdlib.BoundLogger:
    """Return a bound logger with the supplied context."""
    configure()
    return structlog.get_logger(name).bind(**context)
