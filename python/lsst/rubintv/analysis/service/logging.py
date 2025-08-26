import logging
import os

from .data import DataCenter

_persistent_logger = None


def get_persistent_logger(data_center: DataCenter) -> logging.Logger:
    """Get or create a single persistent logger for all file operations.

    This ensures there's only one log file for the entire user workspace.

    Parameters
    ----------
    data_center
        The data center containing the user path root

    Returns
    -------
    logger
        Singleton logger instance
    """
    global _persistent_logger

    if _persistent_logger is None:
        _persistent_logger = logging.getLogger("queries_persistent")
        _persistent_logger.setLevel(logging.INFO)

        log_dir = data_center.logs_path
        os.makedirs(log_dir, exist_ok=True)

        # Single log file for all query operations
        log_file = os.path.join(log_dir, "queries.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        _persistent_logger.addHandler(file_handler)
        # Only log initialization once when logger is first created
        _persistent_logger.info("Query operations logger initialized")

    return _persistent_logger


def cleanup_persistent_logger() -> None:
    """Clean up the persistent logger by closing all file handlers."""
    global _persistent_logger

    if _persistent_logger is not None:
        # Close all file handlers
        for handler in _persistent_logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                _persistent_logger.removeHandler(handler)

        # Reset the global logger
        _persistent_logger = None
