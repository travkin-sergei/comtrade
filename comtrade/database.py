"""
Описани подключение к базе данных

os.environ.get("AIRFLOW_CONN_SANDBOX_COMTRADE") - это простая строка подключения по типу:
    DB_HOST = "localhost"
    DB_PORT = 5432
    DB_NAME = "database"
    DB_USER = "username"
    DB_PASS = "password"

   f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
"""

import os
from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

COMTRADE_KEY = os.getenv('COMTRADE_KEY').split(',')

connection_string = os.getenv('CON_COMTRADE')

# Создание движка
engine = create_engine(connection_string)

# Создание фабрики сессий
session_sync = sessionmaker(bind=engine)
