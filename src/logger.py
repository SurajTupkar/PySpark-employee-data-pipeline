import logging
import os
import time
from datetime import datetime

def get_logger(name):

    # Create logs directory if not exists
    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
    log_dir = os.path.join(project_root, "logs")

    os.makedirs(log_dir, exist_ok=True)

    log_file = f"{log_dir}/app.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate logs
    if logger.handlers:
        return logger

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    formatter.coverter = time.localtime

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File Handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


