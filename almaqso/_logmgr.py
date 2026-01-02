import logging
import logging.handlers
import multiprocessing
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Any
import coloredlogs
from tqdm import tqdm


# class LogManager:
def initialize_log_listener(
    dir: Path,
) -> tuple[str, Queue[Any], logging.handlers.QueueListener]:
    """
    Initialize the LogManager.

    Args:
        dir (Path): Directory to work in.
    """
    now_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    logger_name = f"almaqso-{now_str}"

    manager = multiprocessing.Manager()
    queue: Queue[Any] = manager.Queue()

    fmt = "[%(asctime)s] [%(threadName)s %(processName)s] [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    # --- console handler (with color) ---
    console_handler = logging.StreamHandler()
    colored_formatter = coloredlogs.ColoredFormatter(fmt, datefmt=datefmt)
    console_handler.setFormatter(colored_formatter)

    # --- file handler ---
    path_log_file = dir / f"{logger_name}.log"
    file_handler = logging.FileHandler(path_log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)

    # --- queue listener ---
    listener = logging.handlers.QueueListener(
        queue, console_handler, file_handler, respect_handler_level=True
    )
    listener.start()

    # --- add logger for main process ---
    logger = get_logger_for_subprocess(logger_name, queue)

    logger.info(f"Log file: {path_log_file}")

    tqdm.write = lambda s, *args, **kwargs: logger.info(str(s))

    return logger_name, queue, listener


def stop_log_listener(listener: logging.handlers.QueueListener) -> None:
    """
    Close the logger and stop the listener.
    """
    listener.stop()


def get_logger_for_subprocess(logger_name: str, queue: Queue[Any]) -> logging.Logger:
    """
    Get the logger for subprocesses.

    Returns:
        logging.Logger: The configured logger.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # prevent double logging

    if not any(isinstance(h, logging.handlers.QueueHandler) for h in logger.handlers):
        logger.handlers.clear()
        logger.addHandler(logging.handlers.QueueHandler(queue))

    return logger
