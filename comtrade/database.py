"""
Подключение к базе данных.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

connection = 'postgresql+psycopg2://postgres:123456@localhost:5433/postgres'

engine_sync = create_engine(url=connection, echo=False, )
session_sync = sessionmaker(engine_sync)
