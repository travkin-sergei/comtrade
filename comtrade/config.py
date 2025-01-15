"""
Подключение к базе данных и получение ключей.

os.environ.get("AIRFLOW_CONN_SANDBOX_COMTRADE") - это простая строка подключения по типу:
f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

Настройки логирования занесены сюда же.
"""

import os
import logging
from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

# Получение значения режима логирования
mode = os.getenv('MODE').upper()  # Установите значение по умолчанию на 'DEBUG'

# Сопоставление строки с уровнем логирования
match mode:
    case 'INFO':
        log_level = logging.INFO
    case 'WARNING':
        log_level = logging.WARNING
    case 'ERROR':
        log_level = logging.ERROR
    case 'CRITICAL':
        log_level = logging.CRITICAL
    case _:
        log_level = logging.DEBUG

# Настройка логирования
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
logg = logging.getLogger(__name__)

URL_BASE = os.getenv('URL_BASE', 'https://comtradeapi.un.org')
URL_MONTH = URL_BASE + '/' + 'data/v1/get/C/M/HS'  # os.getenv('URL_MONTH', 'data/v1/get/C/M/HS')
URL_YEAR = URL_BASE + '/' + 'data/v1/get/C/A/HS'  # os.getenv('URL_YEAR', 'data/v1/get/C/A/HS')

CHUNK_SIZE = 100
MAX_COUNT = 100_000  # Лимит количества строк отображения
TIMEOUT = 30  # Ограничение, указанное в документации API

# Настройка базы данных
connection_string = os.getenv('BASE_CONN')

# Создание движка
engine = create_engine(connection_string)

# Создание фабрики сессий
session_sync = sessionmaker(bind=engine)
