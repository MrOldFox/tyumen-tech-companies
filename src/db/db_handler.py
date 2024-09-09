from sqlalchemy import Column, String, Index, Integer
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import insert as pg_insert
from config import Config
from src.utils.logger import logger

Base = declarative_base()

# Асинхронный движок для подключения к базе данных
async_engine = create_async_engine(Config.DATABASE_URL, echo=True)

# Настройка асинхронной сессии
AsyncSessionLocal = sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False
)


class Company(Base):
    """
    Модель для представления компании в базе данных.

    Attributes:
        inn (str): ИНН компании (ключ)
        name (str): Название компании
        okved (str): Код ОКВЭД
        kpp (str): КПП компании
        address (str): Юридический адрес компании
    """
    __tablename__ = 'companies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    inn = Column(String(12), unique=True, nullable=False)
    name = Column(String)
    okved = Column(
        String(300),
        index=True
    )  # возможна доработка: нормализация ОКВЭД через многие ко многим, чтобы не было массы ОКВЭД и лучшей работы индекс
    kpp = Column(String(9))
    address = Column(String)


# Создаем индексы для ускорения поиска
Index('idx_company_okved', Company.okved)
Index('idx_company_inn', Company.inn, unique=True)


async def insert_companies_bulk(session, companies_data, batch_size=1000):
    """
    Асинхронная функция для массовой вставки компаний в БД через батчм.
    """
    try:
        # Проверяем, есть ли данные для вставки
        if not companies_data:
            logger.warning("Нет компаний для вставки.")
            return

        # Разделяем список компаний на батчи
        for i in range(0, len(companies_data), batch_size):
            batch = companies_data[i:i + batch_size]  # Берём следующий батч

            # Формируем запрос на вставку данных
            stmt = pg_insert(Company).values(batch)

            # Если компания уже существует (по ИНН), обновляем её название, ОКВЭД, КПП и адрес
            update_dict = {k: stmt.excluded[k] for k in ['name', 'okved', 'kpp', 'address']}
            stmt = stmt.on_conflict_do_update(index_elements=['inn'], set_=update_dict)

            # Выполняем запрос на вставку и сохраняем изменения
            await session.execute(stmt)
            await session.commit()

        # Логим успешное завершение вставки
        logger.info(f"Успешно добавлены или обновлены {len(companies_data)} компаний.")
    except Exception as e:
        # В случае ошибки откатываем изменения и выводим сообщение об ошибке
        await session.rollback()
        logger.error(f"Ошибка при добавлении компаний: {e}")


async def insert_companies_into_db(companies):
    """
    Функция для вставки списка компаний в базу данных.
    """
    # Открываем асинхронную сессию для работы с базой данных
    async with AsyncSessionLocal() as session:
        try:
            # Используем массовую вставку компаний в базу данных
            await insert_companies_bulk(session, companies)
        except Exception as e:
            # Логируем ошибку, если что-то пошло не так
            logger.error(f"Ошибка при вставке компаний в базу данных: {e}")
            raise


async def init_db():
    """
    Асинхронная функция для создания таблиц в базе данных.
    """
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
