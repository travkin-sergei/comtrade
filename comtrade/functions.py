"""
Список функций приложения.
"""
import sys  # Для получения имени функции
import hashlib

from sqlalchemy import text

from comtrade.config import logg, session_sync, engine
from comtrade.models import Base


def exception_handler(default_value=None):
    """
    Декоратор для обработки исключений.
    :param default_value: Значение, которое будет возвращено в случае исключения. Если None, исключение будет поднято.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as error:
                func_name = sys._getframe().f_code.co_name
                logg.error((f"Ошибка в функции {func_name}: {error}"))
                if default_value is not None:
                    return default_value
                raise

        return wrapper

    return decorator


@exception_handler(default_value="Ошибка")
def hash_sum_256(*args) -> str:
    """Создание SHA-256 хеша. Принимает данные, приводит их к строковому виду,
    переводит в нижний регистр и соединяет в строку через '+'.

    Args:
        *args: Переменное количество аргументов, которые будут хешироваться.

    Returns:
        str: SHA-256 хеш в шестнадцатеричном формате.
    """
    list_str = [str(i).lower() for i in args]
    list_union = '+'.join(list_str)
    return hashlib.sha256(list_union.encode()).hexdigest()


@exception_handler(default_value="Ошибка")
def create_schema_if_not_exists(schema_name):
    session = session_sync()
    try:
        session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
        session.commit()
    except Exception as error:
        session.rollback()
        logg.error((f"Ошибка при создании схемы: {error}"))
        raise
    finally:
        session.close()


@exception_handler(default_value="Ошибка")
def create_table():
    """Создание таблиц базы данных."""

    create_schema_if_not_exists(Base.metadata.schema)
    Base.metadata.create_all(bind=engine)
    logg.info("Таблицы успешно созданы в базе данных.")


@exception_handler(default_value="Ошибка")
def chunk_list(data, chunk_size):
    """Разделение списка на подсписки заданного размера."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

