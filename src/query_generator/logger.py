"""
Logging configuration for mid-size projects with tqdm support.
Usage:
    from myapp.logging_config import setup_logging
    setup_logging(log_file='app.log', log_level='INFO')
"""

import logging
import sys
import time
from dataclasses import dataclass
from enum import Enum
from logging.handlers import RotatingFileHandler
from pathlib import Path

from tqdm import tqdm


class LogLevel(Enum):
  NOTSET = logging.NOTSET  # 0
  DEBUG = logging.DEBUG  # 10
  INFO = logging.INFO  # 20
  WARNING = logging.WARNING  # 30
  ERROR = logging.ERROR  # 40
  CRITICAL = logging.CRITICAL  # 50

  def to_int(self) -> int:
    return self.value

  def to_name(self) -> str:
    return self.name


@dataclass
class LoggingConfig:
  log_file: Path
  log_level: LogLevel = LogLevel.INFO
  console_level: LogLevel = LogLevel.INFO
  file_level: LogLevel = LogLevel.INFO
  max_bytes: int = 10 * 1024 * 1024  # 10MB
  backup_count: int = 5


class TqdmLoggingHandler(logging.Handler):
  """
  Logging handler that writes to tqdm.write() to avoid breaking progress bars.
  """

  def emit(self, record):
    try:
      msg = self.format(record)
      tqdm.write(msg, file=sys.stderr)  # Explicitly write to stderr
      self.flush()
    except Exception:
      self.handleError(record)


def silent_spamming_libraries():
  logging.getLogger("markdown_it").setLevel(logging.INFO)
  logging.getLogger("httpx").setLevel(logging.INFO)
  logging.getLogger("httpcore").setLevel(logging.INFO)


class JupyterFormatter(logging.Formatter):
  def format(self, record):
    ct = self.converter(record.created)
    asctime = time.strftime("%Y-%m-%d %H:%M:%S", ct)
    msecs = int(record.msecs)
    level_char = record.levelname[0]  # First letter: I, W, E, C, D

    return (
      f"[{level_char} {asctime}.{msecs:03d} "
      f"{record.name}] {record.getMessage()}"
    )


def setup_logging(params: LoggingConfig):
  """
  Configure logging for the application.
  """
  # Get Formatters
  file_formatter = JupyterFormatter()
  console_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
  )

  # Get root logger
  root_logger = logging.getLogger()
  root_logger.setLevel(params.log_level.to_int())

  # Remove existing handlers to avoid duplicates
  root_logger.handlers.clear()

  # Console handler with tqdm support
  console_handler = TqdmLoggingHandler()
  console_handler.setLevel(params.console_level.to_int())
  console_handler.setFormatter(console_formatter)
  root_logger.addHandler(console_handler)

  # File handler (rotating)
  file_handler = RotatingFileHandler(
    params.log_file,
    maxBytes=params.max_bytes,
    backupCount=params.backup_count,
    encoding="utf-8",
  )
  file_handler.setLevel(params.file_level.to_int())
  file_handler.setFormatter(file_formatter)
  root_logger.addHandler(file_handler)

  silent_spamming_libraries()

  logger = logging.getLogger(__name__)
  logger.info(
    f"Logging initialized - Level: {params.log_level.to_name()},"
    f" File: {params.log_file}"
  )


def default_logger(
  destination_folder: str,
  *,
  debug_file=False,
  file_name: str = "query_generator.log",
):
  destination_path = Path(destination_folder)
  destination_path.mkdir(parents=True, exist_ok=True)
  log_file = destination_path / file_name
  setup_logging(
    LoggingConfig(
      log_file=log_file,
      log_level=LogLevel.DEBUG if debug_file else LogLevel.INFO,
      console_level=LogLevel.INFO,
      file_level=LogLevel.DEBUG if debug_file else LogLevel.INFO,
    )
  )
