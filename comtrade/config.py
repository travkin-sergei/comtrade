"""
Подключение к базе данных и получение ключей.

os.environ.get("AIRFLOW_CONN_SANDBOX_COMTRADE") - это простая строка подключения по типу:
    DB_HOST = "localhost"
    DB_PORT = 5432
    DB_NAME = "database"
    DB_USER = "username"
    DB_PASS = "password"

   f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
"""
import logging
import os
from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

BASE_URL = 'https://comtradeapi.un.org'
CHUNK_SIZE = 100
MAX_COUNT = 100_000  # Лимит количества строк отображения
TIMEOUT = 30  # Ограничение, указанное в документации API

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

COMTRADE_KEY = os.getenv('COMTRADE_KEY').split(',')

connection_string = os.getenv('CON_COMTRADE')

# Создание движка
engine = create_engine(connection_string)

# Создание фабрики сессий
session_sync = sessionmaker(bind=engine)
