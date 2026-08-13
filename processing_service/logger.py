"""Structured JSON logging module for the processing service.

Provides a configured logger that outputs log entries as valid JSON objects,
enabling consistent, machine-parseable logging across the service. Each log
entry includes a timestamp (ISO 8601), log level, service name, and message.

The log level can be configured via the LOG_LEVEL environment variable
(defaults to INFO).
"""

import json
import logging
import os
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Custom log formatter that outputs each log record as a valid JSON object.

    Each formatted record contains the following fields:
        - timestamp: ISO 8601 formatted UTC timestamp
        - level: Log level name (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        - service: Name of the service that produced the log entry
        - message: The log message
    """

    def __init__(self, service_name: str) -> None:
        """Initialize the JSON formatter with a service name.

        Args:
            service_name: The name of the service to include in log entries.
        """
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a JSON string.

        Args:
            record: The log record to format.

        Returns:
            A valid JSON string representing the log entry.
        """
        log_entry = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "service": self.service_name,
            "message": record.getMessage(),
        }
        return json.dumps(log_entry)


def get_logger(service_name: str) -> logging.Logger:
    """Create and return a logger configured with structured JSON output.

    The logger reads the LOG_LEVEL environment variable to set the logging
    threshold. Valid values are DEBUG, INFO, WARNING, ERROR, and CRITICAL.
    Defaults to INFO if the variable is not set or contains an invalid value.

    Args:
        service_name: The name of the service, included in every log entry.

    Returns:
        A configured logging.Logger instance that outputs JSON-formatted logs.
    """
    logger = logging.getLogger(f"json_logger.{service_name}")

    # Avoid adding duplicate handlers if get_logger is called multiple times
    if logger.handlers:
        return logger

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    numeric_level = getattr(logging, log_level, logging.INFO)

    logger.setLevel(numeric_level)

    handler = logging.StreamHandler()
    handler.setLevel(numeric_level)

    formatter = JSONFormatter(service_name)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    # Prevent log propagation to root logger to avoid duplicate output
    logger.propagate = False

    return logger
