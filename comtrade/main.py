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

import hashlib
import time
import requests
import logging
from sqlalchemy.orm import sessionmaker
from .config import session_sync, engine, COMTRADE_KEY, BASE_URL, MAX_COUNT, CHUNK_SIZE, TIMEOUT
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
)


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
    """Обработка данных полученных от сервера."""

    try:
        response.raise_for_status()
        data = response.json()
        if data.get('count') >= MAX_COUNT:
            save_error_request(dataset_checksum, response.status_code, 413)
        add_param_return(data, dataset_checksum)

    except (requests.HTTPError, ValueError) as error:
        save_error_request(dataset_checksum, response.status_code, 404)
        logging.error(f"Ошибка API Response: {error}")


def record_row(for_url, param: dict, subscription_key: str, dataset_checksum: int, ) -> None:
    """Получение и обработка ответа сервера."""

    url = f"{BASE_URL}/data/v1/get/{for_url.get('typeCode')}/{for_url.get('freqCode')}/HS"

    param["subscription-key"] = subscription_key
    response = requests.get(url, param, verify=False)
    try:
        info_error = response.json().get('error')

        match response.status_code:
            case 200:
                handle_api_response(response, dataset_checksum)
            case 400:
                save_error_request(dataset_checksum, 400, info_error)
            case 429:
                save_error_request(dataset_checksum, 429, response.json().get('message'))
            case 500:
                save_error_request(dataset_checksum, 500, f'Сервер не не может обработа запрос {info_error}')
            case _:
                save_error_request(dataset_checksum, response.status_code, 'Ошибка неизвестна')
                logging.error(f"{response.status_code} = {response.url}")
                exit()

    except requests.RequestException as error:
        logging.error(f"{response.url}: {error}")
        exit()


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
            for_url = {
                "typeCode": i_new.type_code,
                "freqCode": i_new.freq_code,
            }
            # получаем чек сумму устаревших данных
            with session_sync() as session:
                i_old = session.query(ParamReturn.dataset_checksum).filter_by(
                    is_active=True,
                    period=i_new.period,
                    reporter_code=i_new.reporter_code,
                ).first()

            # Получение списка по актуальной версии ТН ВЭД для страны
            with session_sync() as session:
                cmd_code_list = session.query(HsCode.cmd_code).filter_by(
                    is_active=True,
                    hs=i_new.classification_code,
                ).order_by(HsCode.cmd_code).all()

                cmd_codes = [code.cmd_code for code in cmd_code_list]

                # Перебираем список ТН ВЭД для страны
                for chunk in chunk_list(cmd_codes, CHUNK_SIZE):
                    # https://comtradeapi.un.org/data/v1/get/C/A/HS?reporterCode=4&period=2019&partnerCode=156&cmdCode=01&includeDesc=false

                    param = {
                        "reporterCode": i_new.reporter_code,
                        "period": i_new.period,
                        "cmdCode": ','.join(chunk),
                        # "flowCode": get_trade_regimes(), # параметр можно не передавать
                        "maxRecords": "100000",
                        "format": "JSON",
                        "breakdownMode": "classic",
                        "includeDesc": "false",
                    }

                    # Выполняем запрос последовательно для каждого ключа
                    for i_ikey in COMTRADE_KEY:
                        time.sleep(TIMEOUT / len(COMTRADE_KEY))  # 1 ключ не активен 30 секунд.
                        if param.get("cmdCode"):
                            try:
                                record_row(for_url, param, i_ikey, i_new.dataset_checksum)
                            except Exception as error:
                                save_error_request(i_new.dataset_checksum, None, 'Ошибка до запроса')
                                logging.error(f"Ошибка при выполнении запроса: {error}")

            # Обработка ошибок загрузки
            if get_error_request(i_new.dataset_checksum) is None:  # ошибки не найдены
                update_version_data(i_new.dataset_checksum, False)  # Отметить, что версия получена
                if i_old:  # Деактивировать старые записи, если они есть
                    set_param_return(i_old.dataset_checksum, False)
                set_param_return(i_new.dataset_checksum, True)  # Активировать новые записи
            set_error_request(i_new.dataset_checksum, False)  # деактивируем ошибку

    except Exception as error:
        logging.exception(f"Ошибка в main: {error}")
        exit()


if __name__ == '__main__':
    main()
