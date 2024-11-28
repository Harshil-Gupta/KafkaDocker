import logging

def setup_logger(log_file="etl_pipeline.log"):
    """
    Sets up a logger for consistent logging.
    Logs are written both to the console and a file.
    """
    logger = logging.getLogger("KafkaPipeline")
    logger.setLevel(logging.INFO)

    # Ensure we don't add multiple handlers in subsequent calls
    if not logger.hasHandlers():
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Create formatter and attach to handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger