import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

def create_logger(name, sub_dir, prefix):
    # Create the full log directory path
    logs_dir = os.path.join(os.getcwd(), "logs", sub_dir)
    os.makedirs(logs_dir, exist_ok=True)

    # Generate today's date string
    today_str = datetime.now().strftime('%Y-%m-%d')

    # Create the log file with date in the filename
    log_filename = os.path.join(logs_dir, f"{today_str}_{prefix}.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Always add handler if no file with today's date exists
    if not any(isinstance(h, TimedRotatingFileHandler) and h.baseFilename == log_filename for h in logger.handlers):
        handler = TimedRotatingFileHandler(
            log_filename,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8"
        )
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        handler.setLevel(logging.INFO)

        logger.addHandler(handler)
        logger.propagate = False

    return logger

def azure_func_logs():
    http_logger = create_logger("http_sync", "HttpLogs", "Http")
    timer_logger = create_logger("timer_sync", "TimerLogs", "Timer")
    return http_logger, timer_logger

def webhook_logger():
    return create_logger("webhook", "WebhookLogs", "Webhook")

def simple_logger():
    logger = logging.getLogger("SalesforceDataSync")
    logger.setLevel(logging.DEBUG)

    logs_dir = os.path.join(os.getcwd(), "logs", "SimpleLogs")
    os.makedirs(logs_dir, exist_ok=True)

    today_str = datetime.now().strftime('%Y-%m-%d')
    log_filename = os.path.join(logs_dir, f"{today_str}_Simple.log")

    if not logger.handlers:
        handler = TimedRotatingFileHandler(
            log_filename,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8"
        )
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))
        logger.addHandler(handler)

        # Also log to console (optional)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))
        logger.addHandler(stream_handler)

    return logger
