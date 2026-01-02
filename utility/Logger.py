import logging
from logging.handlers import RotatingFileHandler
import sys
from typing import Literal, Optional

sys.path.append('..')
import os
from pathlib import Path

BASE_LOG_DIR = (Path(__file__).resolve().parent.parent / "Log").resolve()


def _resolve_log_directory(path: Optional[os.PathLike[str] | str]) -> Path:
    # Ensure the log directory always stays inside the project Log folder.
    base_dir = BASE_LOG_DIR
    base_dir.mkdir(parents=True, exist_ok=True)

    target_dir = base_dir
    if path not in (None, ".", "./", ".\\"):
        candidate = Path(path)
        if candidate.is_absolute():
            candidate = Path(*candidate.parts[1:])

        resolved_candidate = (base_dir / candidate).resolve()
        try:
            resolved_candidate.relative_to(base_dir)
            target_dir = resolved_candidate
        except ValueError:
            target_dir = base_dir

    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def create_logger(
    name: str,
    level: Literal["debug", "info", "warning"] = "info",
    path: Optional[os.PathLike[str] | str] = ".\\",
):
    logger = logging.getLogger(name)

    if len(logger.handlers) > 0:
        return logger

    match level:
        case "debug":
            level_set = logging.DEBUG
        case "info":
            level_set = logging.INFO
        case "warning":
            level_set = logging.WARNING

    logger.setLevel(level_set)

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s|%(name)s|%(funcName)s:%(lineno)s] %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    log_dir = _resolve_log_directory(path)

    handler = RotatingFileHandler(
        str(log_dir / "Logging.log"),
        maxBytes=1024 * 1024 * 5,
        backupCount=5,
        encoding="UTF-8",
    )
    handler.setFormatter(formatter)
    handler.setLevel(level_set)

    logger.addHandler(handler)

    logger.info("Logger Create")
    return logger
