from config import Config
from src.data_processing.file_processing import get_file, process_zip_file
from src.db.db_handler import init_db
from src.utils.logger import logger
import asyncio


async def main():
    """
    Основная функция для запуска проекта.
    """
    try:
        # Инициализация БД
        logger.info("Инициализация БД...")
        await init_db()
        logger.info("База данных инициализирована.")

        # Проверка и загрузка файла
        logger.info("Проверка и загрузка файла началась...")
        await get_file(Config.URL, Config.ZIP_FILE_PATH)
        logger.info("Файл проверен. Начинаем обработку ZIP файла...")

        # Обработка ZIP файла
        await process_zip_file(Config.ZIP_FILE_PATH)
        logger.info("Обработка ZIP файла завершена.")
    except Exception as e:
        logger.error(f"Ошибка в процессе выполнения программы: {e}")


if __name__ == '__main__':
    # Запускаем основную асинхронную функцию
    asyncio.run(main())
