from dotenv import load_dotenv
import os

load_dotenv()


class Config:
    DATABASE_URL = os.getenv('DATABASE_URL')
    URL = 'https://ofdata.ru/open-data/download/egrul.json.zip'
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    ZIP_FILE_PATH = os.path.join(DATA_DIR, 'egrul.json.zip')
    LOG_DIR = os.path.join(DATA_DIR, 'logs')
    TEST_ZIP_FILE = os.path.join(DATA_DIR, 'test_egrul.zip')