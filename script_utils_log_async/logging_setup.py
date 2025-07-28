import logging
import sys
from typing import Optional

from typing_extensions import override

_shutdown_flag = False


def trigger_shutdown_filter() -> None:
    global _shutdown_flag
    _shutdown_flag = True


def setup_logging(
    level: int = logging.INFO,
    *,
    stderr_level: Optional[int] = logging.ERROR,
    fmt: str = "%(asctime)s [%(levelname)s] %(message)s",
    shutdown_filter: bool = False,
    httpx_filter: bool = False,
) -> None:
    class ShutdownFilter(logging.Filter):
        @override
        def filter(self, record: logging.LogRecord):
            if _shutdown_flag and record.exc_info:
                record.msg = f"[Suppressed stacktrace] {record.getMessage()}"
                record.args = ()
                record.exc_info = None

                if record.levelno > logging.WARNING:
                    record.levelno = logging.WARNING
                    record.levelname = logging.getLevelName(logging.WARNING)
            return True

    logger = logging.getLogger()
    logger.setLevel(level)

    formatter = logging.Formatter(fmt)

    if stderr_level:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.addFilter(lambda record: record.levelno < stderr_level)

        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(stderr_level)

        stdout_handler.setFormatter(formatter)
        stderr_handler.setFormatter(formatter)

        logger.addHandler(stdout_handler)
        logger.addHandler(stderr_handler)
    else:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if shutdown_filter:
        logger.addFilter(ShutdownFilter())

    if httpx_filter:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
