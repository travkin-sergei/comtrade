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

import time
from datetime import datetime
import concurrent.futures
import itertools
import requests

from comtrade.config import (
    logg,
    URL_MONTH,
    MAX_COUNT,
    URL_YEAR,
    TIMEOUT,
)
from comtrade.models import VersionData, HS, Key
from comtrade.functions import create_table
from comtrade.orm import (
    set_version_data,
    set_hs,
    set_partner_areas,
    set_world_statistic,
    set_trade_regimes,
    set_transport_codes,
    set_customs_codes,
    set_plan_request, get_cmd_code,
)


class KeyBlockedException(Exception):
    """Исключение, выбрасываемое при блокировке ключа."""
    pass


def fetch_data_with_retries(url, params, headers, dataset_checksum) -> bool:
    """Извлечение данных с повторными попытками."""
    new_row = {
        "updated_at": datetime.now(),
        "is_active": True,
        "dataset_checksum": dataset_checksum,
        "params": str(params),
        "status_code": 0,
        "count_row": 0,
    }
    params.update(
        {
            "maxRecords": MAX_COUNT,
            "format": "JSON",
            "breakdownMode": "classic",
            "includeDesc": True,
        }
    )
    for attempt in range(10):
        logg.info(f"params: {params}.")
        time.sleep(TIMEOUT)
        try:
            response = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            r_json = response.json()
            new_row["status_code"] = response.status_code
            new_row["count_row"] = max(r_json.get('count', 0), 0)
            match response.status_code:
                case 200:
                    if new_row.get("count_row") < 100_000:
                        new_row["is_active"] = False
                        set_plan_request(new_row)
                        set_world_statistic(r_json, dataset_checksum)
                    return True
                case 400 | 401 | 403 | 429:
                    logg.error(f"Ошибка доступа: {response.status_code}, params: {params}.")
                    Key.set(
                        {
                            "hash_address": headers.get("Ocp-Apim-Subscription-Key"),
                            "is_active": False,
                            "status": response.status_code,
                        }
                    )
                    raise KeyBlockedException(f"Ключ заблокирован с кодом {response.status_code}.")
                case 500:
                    logg.error(f"Ошибка API Response: {response.status_code}, params: {params}.")
                case _:
                    logg.error(f"Неизвестная ошибка API: {response.status_code}, params: {params}.")
                    Key.set(
                        {
                            "hash_address": headers.get("Ocp-Apim-Subscription-Key"),
                            "is_active": False,
                            "status": 0,
                        }
                    )
                    raise KeyBlockedException("Ключ заблокирован из-за неизвестной ошибки.")
            set_plan_request(new_row)
            time.sleep(TIMEOUT)
        except KeyBlockedException:
            raise  # Повторно выбрасываем исключение, чтобы завершить поток
        except requests.exceptions.ConnectionError as conn_err:
            set_plan_request(new_row)
            logg.error(f"Connection error occurred: {conn_err}, params: {params}.")
            time.sleep(TIMEOUT * 5)
        except requests.exceptions.Timeout as timeout_err:
            set_plan_request(new_row)
            logg.error(f"Timeout error occurred: {timeout_err}, params: {params}.")
            time.sleep(TIMEOUT * 5)
        except requests.exceptions.HTTPError as http_err:
            set_plan_request(new_row)
            logg.error(f"HTTP error occurred: {http_err}, params: {params}.")
            time.sleep(TIMEOUT * 5)
    logg.error(f"Exceeded maximum retries for URL: {url}, params: {params}.")
    return False


def get_data(i_vd, i_key):
    """5. Получение данных торговой статистики."""
    try:
        if i_vd is None:
            logg.warning('Нет данных для обработки.')
        else:
            for i_hs in get_cmd_code(i_vd.classification_code):
                headers = {
                    'Cache-Control': 'no-cache',
                    'Ocp-Apim-Subscription-Key': i_key,
                }
                params = {
                    "reporterCode": i_vd.reporter_code,
                    "period": i_vd.period,
                    "cmdCode": ','.join(i_hs),
                }
                url = URL_YEAR if i_vd.freq_code == 'A' else URL_MONTH
                fetch_data_with_retries(url, params, headers, i_vd.dataset_checksum)
        logg.info(f"{i_vd.period, i_vd.reporter_code}. Deactivating VersionData with hash_address: {i_vd.hash_address}")
        VersionData.set({"is_active": False, "hash_address": i_vd.hash_address})
    except KeyBlockedException as e:
        logg.error(f"Поток завершен из-за блокировки ключа: {e}")


def load_directory():
    """Загрузка справочников."""

    # 2.1) Наполнение справочников версий данных
    version = ['A', 'M']
    for i_da in version:
        set_version_data(i_da)
    # 2.2) Наполнение версий справочников гармонизированной системы
    hs_list = ['H0', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6']
    for i_hs in hs_list:
        set_hs(i_hs)
    # 2.3) Наполнение справочника территорий
    set_partner_areas('partnerAreas')
    # 2.4) Наполнение справочника торговых режимов
    set_trade_regimes('tradeRegimes')
    # 2.3) Наполнение справочника видов транспорта
    set_transport_codes('ModeOfTransportCodes')
    # 2.4) Наполнение справочника таможенных процедур
    set_customs_codes('CustomsCodes')


def main():
    """Основной код программы."""
    # 1) Создание таблиц в базе данных
    create_table()
    # 2) Наполнение справочников
    load_directory()
    # 3) Запрос данных торговой статистики
    # 3.1) Запрос списка необходимых стран
    version_data_objects = list(VersionData.get_all({"is_active": True, "period": "2023"}))
    logg.info(f"Найдено объектов: {len(version_data_objects)}")
    num_threads = 2
    for batch in itertools.zip_longest(*[iter(version_data_objects)] * num_threads, fillvalue=None):
        batch = [vd for vd in batch if vd is not None]
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            key_list = [i_key.key for i_key in Key.get_all({"is_active": True})]
            for i, i_vd in enumerate(batch):
                i_key = key_list[i % num_threads]
                futures.append(executor.submit(get_data, i_vd, i_key))
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    logg.info(f"Задача завершена с результатом: {result}")
                except KeyBlockedException as error:
                    logg.error(f"Ошибка при выполнении задачи: {error}")
                except Exception as error:
                    logg.error(f"Неожиданная ошибка: {error}")


if __name__ == '__main__':
    main()
