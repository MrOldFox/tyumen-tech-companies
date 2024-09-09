import os
import zipfile
import json
import aiofiles
import aiohttp
import asyncio
import time
import ijson

from config import Config
from src.db.db_handler import insert_companies_into_db
from src.utils.logger import logger
from datetime import datetime, timedelta


async def is_file_new(file_path=Config.ZIP_FILE_PATH, max_age_days=1):
    """
    Проверяет есть ли с данными.
    """
    # Проверяем, существует ли файл. Если нет, возвращаем True
    if not os.path.exists(file_path):
        return True

    # Файл существует
    return False


async def download_zip_file(url, local_path=Config.ZIP_FILE_PATH, retries=3):
    """
    Скачивает ZIP файл по URL с несколькими попытками.
    """
    for attempt in range(retries):
        try:
            # Открываем сессию и делаем запрос на скачивание файла
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()  # Проверяем успешность запроса
                    # Сохраняем файл по указанному пути
                    async with aiofiles.open(local_path, 'wb') as file:
                        await file.write(await response.read())
            logger.info(f"Файл {local_path} успешно скачан.")
            return  # Успешное завершение
        except aiohttp.ClientError as e:
            # Логим ошибку и продолжаем попытки
            logger.error(f"Ошибка при скачивании файла: {e}")
            if attempt + 1 == retries:
                raise e  # Если это последняя попытка выбрасываем ошибку


async def get_file(url, local_path=Config.ZIP_FILE_PATH):
    """
    Асинхронная функция, которая проверяет, есть ли уже файл.
    Если файл отсутствует, то функция скачает новый.
    """
    # Проверяем, старый ли файл или его вообще нет
    if await is_file_new(local_path):
        # Если файл отсутствует, выводим сообщение в лог и скачиваем его
        logger.info(f"Файл {local_path} отсутствует. Скачиваем новый файл.")
        await download_zip_file(url, local_path)
    else:
        # Если файл в порядке, просто сообщаем об этом
        logger.info(f"Файл {local_path} присутствует. Скачивание не требуется.")


async def process_zip_file(zip_path=Config.ZIP_FILE_PATH):
    """
    Функция обрабатывает ZIP файл, извлекая и анализируя каждый JSON-файл внутри.
    """
    try:
        logger.info(f"Начало обработки ZIP файла: {zip_path}")

        # Проверяем, существует ли файл по указанному пути
        if not os.path.isfile(zip_path):
            raise FileNotFoundError(f"Файл {zip_path} не найден!")

        # Открываем ZIP файл для чтения
        with open(zip_path, mode='rb') as f:
            with zipfile.ZipFile(f) as archive:

                # Получаем список всех JSON-файлов в архиве
                json_files = [file for file in archive.namelist() if file.endswith('.json')]

                # Проверяем, есть ли JSON-файлы в архиве
                if not json_files:
                    raise FileNotFoundError(f"В архиве нет JSON файлов.")

                logger.info(f"Найдено {len(json_files)} JSON файлов для обработки.")

                # Ограничиваем количество одновременно обрабатываемых файлов, сделал минимум 5
                semaphore = asyncio.Semaphore(5)

                # Функция для обработки JSON-файла и записи данных в БД
                async def process_and_save(file):
                    async with semaphore:  # Ограничиваем количество одновременных задач
                        logger.info(f"Начало обработки файла: {file}")
                        start_time = time.time()  # Засекаем время начала обработки файла, для принта
                        companies_found = 0  # Считаем, сколько компаний нашли в файле, также для принта
                        try:
                            # Открываем каждый JSON-файл в архиве
                            with archive.open(file) as json_file:
                                companies = []  # Список для компаний
                                # Обрабатываем файл по частям
                                for batch in process_json_file(json_file):
                                    companies_found += len(batch)  # Увеличиваем счётчик найденных компаний
                                    companies.extend(batch)  # Добавляем компании в список
                                    if len(companies) >= 100:  # Как только набралось 100, записываем в БД
                                        await insert_companies_into_db(companies)
                                        companies = []  # Очищаем список для следующих записей
                                if companies:  # Если остались не записанные компании, то тоже добавляем их в БД
                                    await insert_companies_into_db(companies)
                                full_time = time.time() - start_time  # Считаем, сколько времени заняла обработка, для принта
                                logger.info(f"Завершена обработка файла: {file} за {full_time:.2f} секунд. "
                                            f"Найдено компаний: {companies_found}")
                        except Exception as e:
                            # Логим ошибку, если что-то пошло не так
                            logger.error(f"Ошибка при обработке файла {file}: {e}")

                # Создаём задачи для обработки всех файлов
                tasks = [process_and_save(file) for file in json_files]
                # Ожидаем завершения всех задач
                await asyncio.gather(*tasks)
                logger.info("Обработка ZIP файла завершена.")

    except FileNotFoundError as e:
        # Если файл не найден, логим ошибку
        logger.error(f"Ошибка: {e}")
    except zipfile.BadZipFile as e:
        # Логим ошибку, если архив повреждён
        logger.error(f"Некорректный ZIP файл: {e}")
    except Exception as e:
        # Ловим все остальные возможные ошибки и логим их
        logger.error(f"Ошибка при обработке ZIP файла: {e}")


def process_json_file(json_file):
    """
    Функция для синхронной обработки JSON-файла, извлекает и форматирует информацию о компаниях.
    Возвращает результат порциями.
    """
    try:
        # Парсер для чтения элементов 'item' из JSON-файла
        parser = ijson.items(json_file, 'item')
        processed_companies = []  # Список для хранения данных о компаниях

        # Проходим по каждой компании в файле
        for company in parser:
            # Если данные не в формате словаря, пропускаем такую запись
            if not isinstance(company, dict):
                logger.error(f"Неверный формат данных: ожидался словарь, получен {type(company)}")
                continue

            # Извлекаем ИНН, название и КПП компании
            inn, name, kpp = company.get('inn'), company.get('name'), company.get('kpp')
            company_data = company.get('data', {})

            # Извлекаем все ОКВЭД
            okved_codes = [
                company_data.get('СвОКВЭДОсн', {}).get('КодОКВЭД')  # Основной ОКВЭД
            ] + [
                item.get('КодОКВЭД') for item in company_data.get('СвОКВЭД', {}).get('СвОКВЭДДоп', [])
                if isinstance(item, dict)  # Проверяем, что данные в формате словаря
            ]
            # Оставляем только ОКВЭД коды, которые содержат 62
            okved_codes = [code for code in okved_codes if code and '62' in code]

            # Если компания не связана с разработкой ПО, пропускаем её
            if not okved_codes:
                continue

            # Извлекаем данные об адресе компании
            address_data = company_data.get('СвАдресЮЛ', {}).get('АдресРФ', {})
            # Проверяем, что компания зарегистрирована в Тюмени (наш код региона: 72)
            if not address_data or address_data.get('КодРегион') != '72':
                continue

            # Форматируем адрес компании
            address_parts = [
                address_data.get('Индекс', ''),  # Индекс
                f"{address_data.get('Улица', {}).get('ТипУлица', '')} {address_data.get('Улица', {}).get('НаимУлица', '')}".strip(),  # Улица
                f"д. {address_data.get('Дом', '')}".strip(),  # Дом
                f"корп. {address_data.get('Корпус', '')}" if address_data.get('Корпус') else "",  # Корпус (если есть)
                f"стр. {address_data.get('Стр', '')}" if address_data.get('Стр') else "",  # Строение (если есть)
                f"кв. {address_data.get('Кварт', '')}" if address_data.get('Кварт') else "",  # Квартира (если есть)
            ]
            # Собираем адрес в строку, убирая пустые части
            address = ', '.join(part for part in address_parts if part)

            # Добавляем информацию о компании в список
            processed_companies.append({
                'inn': inn,
                'name': name,
                'okved': ', '.join(okved_codes),
                'kpp': kpp,
                'address': address,
            })

            # Если обработано уже 100 компаний, возвращаем их для записи
            if len(processed_companies) >= 100:
                yield processed_companies
                processed_companies = []  # Очищаем список для следующей порции

        # Если остались необработанные компании, возвращаем их
        if processed_companies:
            yield processed_companies

    # Логим ошибку, если не поулчилось прочитать JSON
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка JSON: {e}")
