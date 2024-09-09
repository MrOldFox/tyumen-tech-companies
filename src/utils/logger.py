import logging
from config import Config


def setup_logging(log_file=Config.LOG_DIR):
    """
    Настраивает логирование.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Создаем хендлер
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Создаем файловый хендлер м задаем уровень логирования: ERROR
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.ERROR)

    # Формат логирования
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Добавляем хендлеры к логгеру
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


logger = setup_logging()