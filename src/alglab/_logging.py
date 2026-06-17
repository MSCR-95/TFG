"""Centralized loguru configuration for all entry points.

Only CLIs (_cli/*.py) should call setup().
Library modules (engine, algorithms, study) only emit messages via
``from loguru import logger``.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from types import FrameType

from loguru import logger


class _InterceptHandler(logging.Handler):
    """Forwards standard Python logging to loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        """Forward *record* to loguru with the correct caller stack depth.

        Args:
            record: Standard library log record to re-emit via loguru.
        """
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        frame: FrameType | None = logging.currentframe()
        depth = 0
        while frame is not None:
            filename = frame.f_code.co_filename
            if depth > 0 and filename != logging.__file__:
                break
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup(*, level: str = "INFO", log_dir: Path | None = None) -> None:
    """Configure loguru for a CLI session.

    Args:
        level: Console log level (DEBUG, INFO, WARNING, ERROR).
        log_dir: Directory for rotating log files. If ``None``, logs only to
            stderr.
    """
    logger.remove()

    level = level.upper()
    logger.add(
        sys.stderr,
        level=level,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    )

    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_dir / "alglab_{time:YYYY-MM-DD}.log",
            rotation="10 MB",
            retention="30 days",
            level="DEBUG",
            enqueue=True,
            encoding="utf-8",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
        )

    # Capture everything going through standard logging (pebble, scipy, etc.)
    logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
