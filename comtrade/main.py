"""
Основной файл приложения.req
https://comtradeapi.un.org/files/v1/app/reference/ListofReferences.json - список справочников.
Для работы необходимо подставить ключи вместо звездочек в COMTRADE_KEY.
Ключи необходимо получить на сайте comtrade.

Из внутренней сети требуется отклить шифрование:
```python
response = requests.get(..., verify=False)
```
CHUNK_SIZE - Количество ТН ВЭД
* Количество потенциальных территорий (не стран)
* количество запрошенных ТН ВЭД (CHUNK_SIZE)
* количество направлений перемещений ("M,X, и т.д.")

`len(PartnerAreas) * CHUNK_SIZE * len("M","X") < 100_000`

verify=False по требованиям безопасности необходимо отключить чтобы трафик можно было просмотреть
Вы у себя удалите этот параметр или переведите verify=True

"""
"""
Основной файл приложения для работы с API Comtrade.
"""

import hashlib
import time
import requests
import logging
import concurrent.futures
from sqlalchemy.orm import sessionmaker
from .config import session_sync, engine, COMTRADE_KEY
from .models import HsCode, Base, ParamReturn
from .orm import (
    add_param_return,
    set_partner_areas,
    set_version_data,
    get_hs_version_data,
    set_hs_code,
    get_country_version_data,
    update_version_data,
    save_error_request,
    get_error_request,
    set_error_request,
    set_param_return,
    set_trade_regimes,
    get_trade_regimes,
    BASE_URL,
)

# Конфигурация
CHUNK_SIZE = 100  # смотри аннотацию выше
MAX_COUNT = 100_000  # Лимит количества строк отображения
TIMEOUT = 30  # Ограничение, указанное в документации API


# Логирование


def create_table():
    """Создание таблиц базы данных."""
    try:
        session = session_sync()
        Base.metadata.create_all(engine)
        session.commit()
    except Exception as error:
        logging.exception(f"Ошибка в create_table: {error}")


def hash_sum_256(*args):
    """Создание SHA256 хеша для строки."""
    try:
        list_str = [str(i).lower() for i in args]
        list_union = '+'.join(list_str)
        return hashlib.sha256(list_union.encode()).hexdigest()
    except Exception as error:
        logging.exception(f"Ошибка в hash_sum_256: {error}")


def chunk_list(data, chunk_size):
    """Разделение списка на подсписки заданного размера."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def handle_api_response(response, dataset_checksum):
    """Обработка ответа API и сохранение данных."""

    try:
        response.raise_for_status()
        data = response.json()
        if data.get('count', 0) >= MAX_COUNT:
            save_error_request(dataset_checksum, response.status_code, 413)
        add_param_return(data, dataset_checksum)
    except (requests.HTTPError, ValueError) as error:
        save_error_request(dataset_checksum, response.status_code, 404)
        logging.error(f"Ошибка API Response: {error}")


def record_row(param: dict, dataset_checksum: int, subscription_key: str) -> None:
    """Запись данных в таблицу ParamReturn."""

    url = f"{BASE_URL}/data/v1/get/{param['typeCode']}/{param['freqCode']}/HS"
    param.pop("typeCode")
    param.pop("freqCode")
    param["subscription-key"] = subscription_key

    try:
        response = requests.get(url, param, verify=False)
        if response.status_code == 200:
            handle_api_response(response, dataset_checksum)
        else:
            save_error_request(dataset_checksum, response.status_code, response.json().get('statusCode'))
    except requests.RequestException as error:
        logging.error(f"Ошибка в запросе: {error}")
    finally:
        time.sleep(TIMEOUT)


def main() -> None:
    """Основная функция для загрузки и обработки данных."""
    session_sync = sessionmaker(bind=engine)

    try:
        # Шаг 1: Инициализация базы данных
        create_table()

        # Шаг 2: Загрузка справочной информации
        set_partner_areas()  # загружаем справочник территорий
        set_trade_regimes()  # загружаем справочник торговых режимов
        for i_da in ['M', 'A']:
            set_version_data(i_da)  # загружаем справочник актуальных данных помесячный или годовой
        for i_hs in get_hs_version_data():
            set_hs_code(i_hs)  # загружаем справочник ТН ВЭД разных версий

        # Шаг 3: Обработка кодов стран
        for i_new in get_country_version_data():

            # получаем чек сумму устаревших данных
            with session_sync() as session:
                i_old = session.query(ParamReturn.dataset_checksum).filter_by(
                    is_active=True,
                    period=i_new.period,
                    reporter_code=i_new.reporter_code,
                ).first()
                if i_old:
                    set_param_return(
                        checksum=i_old.dataset_checksum,
                        is_active=False,
                    )

            # Получение списка по актуальной версии ТН ВЭД для страны
            with session_sync() as session:
                cmd_code_list = session.query(HsCode.cmd_code).filter_by(
                    is_active=True,
                    hs=i_new.classification_code,
                ).order_by(HsCode.cmd_code).all()

                cmd_codes = [code.cmd_code for code in cmd_code_list]

                # Перебираем список ТН ВЭД для страны
                for chunk in chunk_list(cmd_codes, CHUNK_SIZE):
                    param = {
                        "typeCode": i_new.type_code,
                        "freqCode": i_new.freq_code,
                        "reporterCode": i_new.reporter_code,
                        "cmdCode": ','.join(chunk),
                        "flowCode": get_trade_regimes(),
                        "period": i_new.period,
                        "maxRecords": "100000",
                        "format": "JSON",
                        "breakdownMode": "classic",
                        "includeDesc": "True",
                    }

                    # Используем ThreadPoolExecutor для параллельных запросов
                    with concurrent.futures.ThreadPoolExecutor(max_workers=len(COMTRADE_KEY)) as executor:
                        futures = []
                        for i_ikey in COMTRADE_KEY:
                            futures.append(executor.submit(record_row, param, i_new.dataset_checksum, i_ikey))

                        # Ждем завершения всех потоков
                        for future in concurrent.futures.as_completed(futures):
                            try:
                                future.result()  # Получаем результат, чтобы поймать возможные исключения
                            except Exception as error:
                                logging.error(f"Ошибка в потоке: {error}")

            # Обработка ошибок загрузки
            if get_error_request(i_new.dataset_checksum) is None:  # ошибки не найдены
                update_version_data(i_new.dataset_checksum, False)  # Отметить, что версия получена
                set_param_return(i_old.dataset_checksum, False)  # Деактивировать старые записи
                set_param_return(i_new.dataset_checksum, True)  # Активировать новые записи

            set_error_request(i_new.dataset_checksum, False)  # деактивируем ошибку

    except Exception as error:
        logging.exception(f"Ошибка в main: {error}")


if __name__ == '__main__':
    main()
