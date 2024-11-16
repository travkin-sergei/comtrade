"""
Основной файл приложения.
https://comtradeapi.un.org/files/v1/app/reference/ListofReferences.json - список справочников.
"""

import hashlib
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from comtrade.database import session_sync, engine_sync
from comtrade.log import logConnect
from comtrade.models import Code, Base
from comtrade.orm import (
    orm_param_return,
    save_partner_areas,
    save_version_data,
    get_hs_version_data,
    save_hs,
    get_country_version_data,
    update_version_data,
)

CHUNK_SIZE = 100
TIMEOUT = 30
BASE_URL = 'https://comtradeapi.un.org'

SUBSCRIPTION_KEYS = [
    '************************',  #
    '************************',  #
]

# Настройка логирования
log = logConnect()


def hash_sum_256(*args):
    """Создание Хеш суммы по строке."""
    try:
        list_str = [str(i).lower() for i in args]
        list_union = '+'.join(list_str)
        ha256 = hashlib.sha256(list_union.encode()).hexdigest()
        return ha256
    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}: {error}')


def chunk_list(data, chunk_size):
    """Разбивает список на подсписки заданного размера."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def record_row(param: dict, dataset_checksum: int, subscription_key: str) -> None:
    """Запись данных в таблицу ParamReturn"""
    url = f"{BASE_URL}/data/v1/get/{param['typeCode']}/{param['freqCode']}/HS"
    param.pop("typeCode")
    param.pop("freqCode")
    param["subscription-key"] = subscription_key

    try:
        response = requests.get(url, param)
        response.raise_for_status()  # Проверка на успешный ответ
        orm_param_return(response.json(), dataset_checksum)
    except requests.RequestException as e:
        log.error(f"Error fetching data: {e}")
    finally:
        time.sleep(TIMEOUT)


def main():
    try:

        Base.metadata.create_all(engine_sync)
        save_version_data(requests.get(f'{BASE_URL}/public/v1/getDA/C/A/HS').json())
        save_partner_areas(requests.get(f'{BASE_URL}/files/v1/app/reference/partnerAreas.json').json())

        for hs in get_hs_version_data():
            save_hs(requests.get(f'{BASE_URL}/files/v1/app/reference/{hs}.json').json(), hs)

        for i_version_data in get_country_version_data():
            with session_sync() as session:
                cmd_code_list = session.query(Code.cmd_code).filter_by(
                    is_active=True,
                    hs=i_version_data.classification_code
                ).order_by(Code.cmd_code).all()

                cmd_codes = [code.cmd_code for code in cmd_code_list]

                # Используем ThreadPoolExecutor для параллельных запросов
                with ThreadPoolExecutor(max_workers=len(SUBSCRIPTION_KEYS)) as executor:
                    futures = []
                    for chunk in chunk_list(cmd_codes, CHUNK_SIZE):
                        param = {
                            "typeCode": i_version_data.type_code,
                            "freqCode": i_version_data.freq_code,
                            "reporterCode": i_version_data.reporter_code,
                            "cmdCode": ','.join(chunk),
                            "flowCode": "M,X",
                            "period": i_version_data.period,
                            "maxRecords": "100000",
                            "format": "JSON",
                            "breakdownMode": "classic",
                            "includeDesc": "True",
                        }
                        # Получаем следующий ключ для использования
                        subscription_key = SUBSCRIPTION_KEYS[len(futures) % len(SUBSCRIPTION_KEYS)]
                        futures.append(
                            executor.submit(record_row, param, i_version_data.dataset_checksum, subscription_key)
                        )

                    # Ожидаем завершения всех запросов
                    for future in as_completed(futures):
                        try:
                            future.result()  # Проверка на исключения
                        except Exception as e:
                            log.error(f"Error in future: {e}")

            update_version_data(i_version_data.dataset_checksum)

    except Exception as error:
        log.exception(f'Error in main: {error}')


if __name__ == '__main__':
    main()
else:
    raise SystemExit
