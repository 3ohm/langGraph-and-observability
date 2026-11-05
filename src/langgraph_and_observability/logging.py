"""JSON logs on stdout, one object per line, through structlog.

Each line has ``timestamp`` (ISO 8601, UTC), ``level``, ``logger`` and ``event``, plus
the logger's bound fields and the ``structlog.contextvars`` bound at that moment.
Records from the standard ``logging`` module, as libraries write them, get the same
JSON form, with the same contextvars. The worker's run executor binds ``run_id``,
``agent`` and ``attempt`` while a run executes, so each line logged inside a run names
it (spec §12.1).
"""

import logging
import sys

import structlog
from structlog.typing import Processor

from langgraph_and_observability.settings import Settings

# These run for structlog events and for records from the standard logging module.
_SHARED_PROCESSORS: list[Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="iso", utc=True),
]


class _StdoutHandler(logging.Handler):
    """Writes each record to whatever ``sys.stdout`` is at that moment.

    logging's last-resort handler does the same for stderr. Output follows
    redirection, and the handler never keeps a reference to a stream that was closed.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            sys.stdout.write(self.format(record) + "\n")
            sys.stdout.flush()
        except Exception:
            self.handleError(record)


def configure_logging(settings: Settings) -> None:
    """Send all logs to stdout as JSON at INFO and above. Safe to call again.

    ``settings`` is accepted so later configuration can tune logging; v1 output is
    the same in every env.
    """
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *_SHARED_PROCESSORS,
            structlog.processors.StackInfoRenderer(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        # A logger used before this call must still pick up this configuration.
        cache_logger_on_first_use=False,
    )
    handler = _StdoutHandler()
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=_SHARED_PROCESSORS,
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                # Plain-text tracebacks: frame locals could hold secrets.
                structlog.processors.format_exc_info,
                # default=str: UUIDs, paths and datetimes render as plain strings.
                structlog.processors.JSONRenderer(default=str),
            ],
        )
    )
    root = logging.getLogger()
    for old in [h for h in root.handlers if isinstance(h, _StdoutHandler)]:
        root.removeHandler(old)
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger that writes through the standard logger ``name``."""
    return structlog.stdlib.get_logger(name)
