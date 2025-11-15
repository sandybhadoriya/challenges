# File: src/my_package/logging.py
import logging
import json
from datetime import datetime, timezone
import sys
# Import the configuration module
from .config import config

class StructuredFormatter(logging.Formatter):
    """Structured JSON logging for observability (Req. 12)."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "pathname": record.pathname,
            # Add environment context from config
            "environment": config.environment,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


def setup_logging(level: str = config.log_level):
    """Initializes structured logging for the application."""
    # Remove existing handlers
    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)
            
    # Set the main package logger
    logger = logging.getLogger("my_package")
    logger.setLevel(level.upper())
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(StructuredFormatter())
    logger.addHandler(handler)
    
    # Also set root handler to the structured one for unhandled logs
    root_logger.setLevel(level.upper())
    root_logger.addHandler(handler)
    
    return logger

# Initialize logging immediately using the config environment variable
logger = setup_logging()