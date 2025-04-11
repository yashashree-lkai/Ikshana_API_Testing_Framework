import logging
import os
import datetime
from logging.handlers import RotatingFileHandler

import config

# Ensure directories exist
os.makedirs(config.config_loader.LOG_DIR, exist_ok=True)
os.makedirs(config.config_loader.REPORTS_DIR, exist_ok=True)

# Log file
LOG_FILE = os.path.join(config.config_loader.LOG_DIR, 'api_automation.log')
# Generate a unique filename for the report
timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
REPORTS_FILE = os.path.join(config.config_loader.REPORTS_DIR, f'test_report_{timestamp}.html')

logger_1 = logging.getLogger(__name__)


# Logging setup
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # File handler with rotation
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(message)s'))

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


