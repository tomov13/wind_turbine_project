import logging
import sys
from typing import Optional

class LoggerUtility:
    """Centralized Logger Utility"""

    LOG_LEVEL = logging.INFO
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def setup_logging(log_level: Optional[int] = None) -> logging.Logger:
        """Setup and return a logger that prints to stdout so it works across `%run` notebooks"""

        logger = logging.getLogger(__name__)

        # Remove all handlers (fixes duplicate logging issues in Databricks)
        logger.handlers.clear()

        logger.setLevel(log_level or LoggerUtility.LOG_LEVEL)

        # Ensure logs are printed to stdout, so they show in notebooks
        stream_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(LoggerUtility.LOG_FORMAT, datefmt=LoggerUtility.DATE_FORMAT)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        return logger
