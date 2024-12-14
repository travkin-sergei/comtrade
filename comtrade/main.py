"""
Основной файл приложения.req
https://comtradeapi.un.org/files/v1/app/reference/ListofReferences.json - список справочников.
Для работы необходимо подставить ключи вместо звездочек в SUBSCRIPTION_KEYS.
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
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor, as_completed
from .database import session_sync, engine, COMTRADE_KEY
from .models import HsCode, Base, ParamReturn
from .orm import (
    add_param_return,
    save_partner_areas,
    save_version_data,
    get_hs_version_data,
    save_hs_code,
    get_country_version_data,
    update_version_data,
    save_error_request,
    get_error_request,
    update_error_request,
    update_param_return,
    save_trade_regimes,
    get_trade_regimes,
)

# Конфигурация
CHUNK_SIZE = 100
MAX_COUNT = 100_000  # Лимит количества строк отображения
TIMEOUT = 30  # Ограничение, указанное в документации API
BASE_URL = 'https://comtradeapi.un.org'
SUBSCRIPTION_KEYS = COMTRADE_KEY

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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
            raise ValueError("Превышено значение MAX_COUNT. Настройте размер CHUNK_SIZE или фильтры.")
        add_param_return(data, dataset_checksum)
    except (requests.HTTPError, ValueError) as e:
        save_error_request(dataset_checksum, response.status_code, str(e))
        logging.error(f"Ошибка API Response: {e}")


def record_row(param: dict, dataset_checksum: int, subscription_key: str) -> None:
    """Запись данных в таблицу ParamReturn."""
    url = f"{BASE_URL}/data/v1/get/{param['typeCode']}/{param['freqCode']}/HS"
    param.pop("typeCode")
    param.pop("freqCode")
    param["subscription-key"] = subscription_key

    try:
        response = requests.get(url, param, verify=False)
        handle_api_response(response, dataset_checksum)
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
        save_version_data(requests.get(f'{BASE_URL}/public/v1/getDA/C/A/HS', verify=False).json())
        save_partner_areas(requests.get(f'{BASE_URL}/files/v1/app/reference/partnerAreas.json', verify=False).json())
        save_trade_regimes(requests.get(f'{BASE_URL}/files/v1/app/reference/tradeRegimes.json', verify=False).json())

        # Шаг 3: Обработка версий HS
        for hs in get_hs_version_data():
            save_hs_code(requests.get(f'{BASE_URL}/files/v1/app/reference/{hs}.json', verify=False).json(), hs)

        # Шаг 4: Обработка кодов стран
        for i_version in get_country_version_data():
            # Снятие активности с предыдущих записей
            with session_sync() as session:
                checksum_old = session.query(ParamReturn.dataset_checksum).filter_by(
                    is_active=True, period=i_version.period, reporter_code=i_version.reporter_code
                ).first()
                if checksum_old:
                    update_param_return(checksum=checksum_old.dataset_checksum, is_active=False)

            # Получение списка ТН ВЭД
            with session_sync() as session:
                cmd_code_list = session.query(HsCode.cmd_code).filter_by(
                    is_active=True,
                    hs=i_version.classification_code
                ).order_by(HsCode.cmd_code).all()

                cmd_codes = [code.cmd_code for code in cmd_code_list]

                # Параллельная обработка запросов
                with ThreadPoolExecutor(max_workers=len(SUBSCRIPTION_KEYS)) as executor:
                    futures = []
                    for chunk in chunk_list(cmd_codes, CHUNK_SIZE):
                        param = {
                            "typeCode": i_version.type_code,
                            "freqCode": i_version.freq_code,
                            "reporterCode": i_version.reporter_code,
                            "cmdCode": ','.join(chunk),
                            "flowCode": get_trade_regimes(),
                            "period": i_version.period,
                            "maxRecords": "100000",
                            "format": "JSON",
                            "breakdownMode": "classic",
                            "includeDesc": "True",
                        }
                        subscription_key = SUBSCRIPTION_KEYS[len(futures) % len(SUBSCRIPTION_KEYS)]
                        futures.append(executor.submit(record_row, param, i_version.dataset_checksum, subscription_key))

                    # Ожидание завершения запросов
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as error:
                            logging.error(f"Ошибка в будущем потоке: {error}")

            # Обновление статуса версии данных
            update_version_data(i_version.dataset_checksum, False)

            # Обработка ошибок загрузки
            for i_error in get_error_request():
                if i_error:
                    update_version_data(i_error.dataset_checksum, True)
                    update_error_request(i_error.dataset_checksum)
                    update_param_return(i_error.dataset_checksum, False)
                else:
                    update_param_return(checksum_old.dataset_checksum, True)

    except Exception as error:
        logging.exception(f"Ошибка в main: {error}")

main()