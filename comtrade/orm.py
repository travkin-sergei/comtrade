"""
Операцию сожержит описание операции с моделью данных.

Обратите внимание на hash_sum_256. Эта функция для получения адреса данных
"""

import json
import sys
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

from .config import logg, URL_BASE, session_sync, connection_string
from .functions import hash_sum_256, exception_handler
from .models import (
    VersionData,
    VersionDirectory,
    WorldStatistic,
    PartnerAreas,
    HS,
    TradeRegimes,
    TransportCodes,
    CustomsCodes,
    PlanRequest, session_scope,
)


@exception_handler(default_value="Ошибка")
def set_version_directory(table_name: str, alias: str, tab_list: json) -> bool:
    """Проверка. Таблицу надо обновлять?"""

    table_obj = VersionDirectory

    new_row = {
        "updated_at": datetime.now(),
        "is_active": True,
        "hash_address": hash_sum_256(alias),
        "alias": alias,
        "table_name": table_name,
        "tab_hash": hash_sum_256(str(tab_list)),
    }

    with session_sync() as session:
        try:
            # Получаем объект по имени таблицы
            old_obj = session.query(table_obj).filter_by(hash_address=new_row['hash_address']).first()

            if old_obj is None:
                # Добавляем объект запись если его нет
                stmt = table_obj(**new_row)
                session.add(stmt)
                session.commit()
                logg.info(f'def {sys._getframe().f_code.co_name}. Запись добавлена для {table_name}.')
                return True

            # Если данные совпали, то ничего не меняем
            if old_obj.tab_hash == new_row['tab_hash'] and old_obj.hash_address == new_row['hash_address']:
                logg.info(f'def {sys._getframe().f_code.co_name}. Данные совпадают для {table_name}.')
                return False

            # Обновляем все записи, соответствующие hash_address
            session.query(table_obj).filter_by(hash_address=new_row['hash_address']).update(
                {"tab_hash": new_row['tab_hash']})
            session.commit()
            logg.info(f'def {sys._getframe().f_code.co_name}. Запись обновлена для {table_name}.')
            return True

        except Exception as e:
            logg.error(f'Ошибка при работе с таблицей {table_name}: {e}')
            raise  # Можно выбросить исключение дальше, если это необходимо


@exception_handler(default_value="Ошибка")
def set_version_data(alias) -> None:
    """Сохранение существующих версий данных."""

    table_obj = VersionData
    table_name = table_obj.__tablename__

    data_json = requests.get(f'{URL_BASE}/public/v1/getDA/C/{alias}/HS', verify=False).json().get('data')

    if set_version_directory(table_name, alias, data_json):
        for i_data in data_json:
            new_row = {
                "updated_at": datetime.now(),
                "is_active": True,
                "hash_address": hash_sum_256(
                    i_data.get('typeCode'),
                    i_data.get('freqCode'),
                    i_data.get('period'),
                    i_data.get('reporterCode'),
                    i_data.get('firstReleased'),
                    i_data.get('lastReleased'),
                ),
                "dataset_code": i_data.get('datasetCode'),
                "type_code": i_data.get('typeCode'),
                "freq_code": i_data.get('freqCode'),
                "period": i_data.get('period'),
                "reporter_code": i_data.get('reporterCode'),
                "reporter_iso": i_data.get('reporterISO'),
                "reporter_desc": i_data.get('reporterDesc'),
                "classification_code": i_data.get('classificationCode'),
                "classification_search_code": i_data.get('classificationSearchCode'),
                "is_original_classification": i_data.get('isOriginalClassification'),
                "is_extended_flow_code": i_data.get('isExtendedFlowCode'),
                "is_extended_partner_code": i_data.get('isExtendedPartnerCode'),
                "is_extended_partner2_code": i_data.get('isExtendedPartner2Code'),
                "is_extended_cmd_code": i_data.get('isExtendedCmdCode'),
                "is_extended_customs_code": i_data.get('isExtendedCustomsCode'),
                "is_extended_mot_code": i_data.get('isExtendedMotCode'),
                "total_records": i_data.get('totalRecords'),
                "dataset_checksum": i_data.get('datasetChecksum'),
                "first_released": i_data.get('firstReleased'),
                "last_released": i_data.get('lastReleased'),
            }

            with session_sync() as session:
                old_obj = session.query(table_obj).filter_by(dataset_code=new_row.get("dataset_code")).first()

                if old_obj is None:
                    # Если данные отсутствуют, записываем новую строку
                    session.add(table_obj(**new_row))
                    session.commit()
                elif old_obj.dataset_checksum != new_row.get("dataset_checksum"):
                    # Если контрольная сумма не совпадает, обновляем строку
                    session.query(table_obj).filter_by(dataset_code=new_row.get("dataset_code")).update(new_row)
                    session.commit()
                else:
                    logg.error((f'def {sys._getframe().f_code.co_name}. An incredible mistake'))


@exception_handler(default_value="Ошибка")
def set_world_statistic(initial_json: json, dataset_checksum: int) -> None:
    """Добавление данных в таблицу WorldStatistic"""

    table_obj = WorldStatistic
    data_to_insert = []

    for i_json in initial_json.get('data'):
        new_row = {
            "updated_at": datetime.now(),
            "is_active": False,  # переводим в True только после полной записи страны
            "type_code": i_json.get('typeCode'),
            "freq_code": i_json.get('freqCode'),
            "period": i_json.get('period'),
            "reporter_code": i_json.get('reporterCode'),
            "flow_code": i_json.get('flowCode'),
            "partner_code": i_json.get('partnerCode'),
            "partner2_code": i_json.get('partner2Code'),
            "classification_code": i_json.get('classificationCode'),
            "classification_search_code": i_json.get('classificationSearchCode'),
            "is_original_classification": i_json.get('isOriginalClassification'),
            "cmd_code": i_json.get('cmdCode'),
            "cmd_desc": i_json.get('cmdDesc'),
            "aggr_level": i_json.get('aggrLevel'),
            "is_leaf": i_json.get('isLeaf'),
            "customs_code": i_json.get('customsCode'),
            "mos_code": i_json.get('mosCode'),
            "mot_code": i_json.get('motCode'),
            "qty_unit_code": i_json.get('qtyUnitCode'),
            "qty_unit_abbr": i_json.get('qtyUnitAbbr'),
            "qty": i_json.get('qty'),
            "is_qty_estimated": i_json.get('isQtyEstimated'),
            "alt_qty_unit_code": i_json.get('altQtyUnitCode'),
            "alt_qty_unit_abbr": i_json.get('altQtyUnitAbbr'),
            "alt_qty": i_json.get('altQty'),
            "is_alt_qty_estimated": i_json.get('isAltQtyEstimated'),
            "net_wgt": i_json.get('netWgt'),
            "is_net_wgt_estimated": i_json.get('isNetWgtEstimated'),
            "gross_wgt": i_json.get('grossWgt'),
            "is_gross_wgt_estimated": i_json.get('isGrossWgtEstimated'),
            "cif_value": i_json.get('cifvalue'),
            "fob_value": i_json.get('fobvalue'),
            "primary_value": i_json.get('primaryValue'),
            "legacy_estimation_flag": i_json.get('legacyEstimationFlag'),
            "is_reported": i_json.get('isReported'),
            "is_aggregate": i_json.get('isAggregate'),
            "dataset_checksum": dataset_checksum,
        }
        # Добавляем новую строку в список
        data_to_insert.append(new_row)

        # Создаем DataFrame из списка словарей
    df = pd.DataFrame(data_to_insert)

    # Вставка данных в таблицу
    with create_engine(connection_string).connect() as connection:
        df.to_sql(
            table_obj.__tablename__,
            con=connection,
            schema=table_obj.__table__.schema,
            if_exists='append',
            index=False
        )

    logg.info(f'def {sys._getframe().f_code.co_name} inserted {len(df)} rows into {table_obj.__tablename__}')


@exception_handler(default_value="Ошибка")
def set_partner_areas(alais) -> None:
    """Добавление данных в таблицу partnerAreas."""

    table_obj = PartnerAreas
    table_name = table_obj.__tablename__

    url = f'{URL_BASE}/files/v1/app/reference/{alais}.json'
    data_json = requests.get(url, verify=False).json().get('results')

    # Проверить существует ли талица в списке в таком виде
    if set_version_directory(table_name, alais, data_json):
        # Устанавливаем все записи is_active в False
        with session_sync() as session:
            session.query(table_obj).update({"is_active": False})
            session.commit()

        for i_json in data_json:

            new_row = {
                "updated_at": datetime.now(),
                "is_active": True,
                "hash_address": hash_sum_256(i_json.get("id")),
                "foreign_id": i_json.get('id'),
                "text": i_json.get('text'),
                "partner_code": i_json.get('PartnerCode'),
                "partner_desc": i_json.get('PartnerDesc'),
                "partner_note": i_json.get('partnerNote'),
                "partner_code_iso_alpha2": i_json.get('PartnerCodeIsoAlpha2'),
                "partner_code_iso_alpha3": i_json.get('PartnerCodeIsoAlpha3'),
                "entry_effective_date": i_json.get('entryEffectiveDate'),
                "is_group": i_json.get('isGroup'),
            }
            with session_sync() as session:
                old_obj = session.query(table_obj).filter_by(hash_address=new_row.get("hash_address")).first()
                if old_obj is None:
                    stmt = table_obj(**new_row)
                    session.add(stmt)
                    session.commit()
                    logg.info(f"def {sys._getframe().f_code.co_name} row add: {new_row.get("hash_address")}")
                else:
                    old_obj.is_active = True
                    session.commit()
                    logg.info(
                        f"def {sys._getframe().f_code.co_name} row duplication: {new_row.get("hash_address")}")


@exception_handler(default_value="Ошибка")
def set_hs(alias) -> None:
    """Запись данных версии кодов ТН ВЭД."""

    table_obj = HS
    table_name = table_obj.__tablename__

    url = f'{URL_BASE}/files/v1/app/reference/{alias}.json'

    incoming_data = requests.get(url, verify=False).json()
    data_json = incoming_data.get('results')  # Список кодов ТН ВЭД

    if set_version_directory(table_name, alias, data_json):
        with session_sync() as session:
            session.query(table_obj).filter(table_obj.hs == alias).update({"is_active": False})
            session.commit()

            for i_data in data_json:

                i_data["hash_address"] = hash_sum_256(
                    i_data.get('id'),
                    alias,
                )

                new_row = {
                    "updated_at": datetime.now(),
                    "is_active": True,
                    "hash_address": i_data.get('hash_address'),
                    "hs": alias,
                    "cmd_code": i_data.get('id'),
                    "text": i_data.get('text'),
                    "parent": i_data.get('parent'),
                    "is_leaf": i_data.get('isLeaf'),
                    "aggr_level": i_data.get('aggrLevel'),
                    "standard_unit_abbr": i_data.get('standardUnitAbbr'),
                }

                old_obj = session.query(table_obj).filter_by(hash_address=new_row.get("hash_address")).first()
                if old_obj is None:
                    stmt = table_obj(**new_row)
                    session.add(stmt)
                    session.commit()
                    logg.info(f"def {sys._getframe().f_code.co_name} row add: {new_row.get('hash_address')}")
                else:
                    old_obj.is_active = True
                    logg.info(
                        f"def {sys._getframe().f_code.co_name} row duplication: {new_row.get('hash_address')}")


@exception_handler(default_value="Ошибка")
def set_trade_regimes(alais) -> None:
    """Добавление данных в таблицу TradeRegimes."""

    table_obj = TradeRegimes
    table_name = table_obj.__tablename__
    '                 /files/v1/app/reference/tradeRegimes.json'
    url = f'{URL_BASE}/files/v1/app/reference/{alais}.json'
    data_json = requests.get(url, verify=False).json().get('results')

    # Проверить существует ли талица в списке в таком виде
    if set_version_directory(table_name, alais, data_json):
        # Устанавливаем все записи is_active в False
        with session_sync() as session:
            session.query(table_obj).update({"is_active": False})
            session.commit()

        for i_json in data_json:

            new_row = {
                "updated_at": datetime.now(),
                "is_active": True,
                "hash_address": hash_sum_256(i_json.get("id")),
                "flow_code": i_json.get('id'),
                "flow_desc": i_json.get('text'),
            }
            with session_sync() as session:
                old_obj = session.query(table_obj).filter_by(hash_address=new_row.get("hash_address")).first()
                if old_obj is None:
                    stmt = table_obj(**new_row)
                    session.add(stmt)
                    session.commit()
                    logg.info(f"def {sys._getframe().f_code.co_name} row add: {new_row.get("hash_address")}")
                else:
                    old_obj.is_active = True
                    session.commit()
                    logg.info(
                        f"def {sys._getframe().f_code.co_name} row duplication: {new_row.get("hash_address")}")


@exception_handler(default_value="Ошибка")
def set_transport_codes(alais) -> None:
    """Добавление данных в таблицу TransportCodes."""

    table_obj = TransportCodes
    table_name = table_obj.__tablename__

    url = f'{URL_BASE}/files/v1/app/reference/{alais}.json'
    data_json = requests.get(url, verify=False).json().get('results')

    # Проверить существует ли талица в списке в таком виде
    if set_version_directory(table_name, alais, data_json):
        # Устанавливаем все записи is_active в False
        with session_sync() as session:
            session.query(table_obj).update({"is_active": False})
            session.commit()

        for i_json in data_json:

            new_row = {
                "updated_at": datetime.now(),
                "is_active": True,
                "hash_address": hash_sum_256(i_json.get("id")),
                "foreign_id": i_json.get('id'),
                "text": i_json.get('text'),

            }
            with session_sync() as session:
                old_obj = session.query(table_obj).filter_by(hash_address=new_row.get("hash_address")).first()
                if old_obj is None:
                    stmt = table_obj(**new_row)
                    session.add(stmt)
                    session.commit()
                    logg.info(f"def {sys._getframe().f_code.co_name} row add: {new_row.get("hash_address")}")
                else:
                    old_obj.is_active = True
                    session.commit()
                    logg.info(
                        f"def {sys._getframe().f_code.co_name} row duplication: {new_row.get("hash_address")}")


@exception_handler(default_value="Ошибка")
def set_customs_codes(alais) -> None:
    """Добавление данных в таблицу CustomsCodes."""

    table_obj = CustomsCodes
    table_name = table_obj.__tablename__

    url = f'{URL_BASE}/files/v1/app/reference/{alais}.json'
    data_json = requests.get(url, verify=False).json().get('results')

    # Проверить существует ли талица в списке в таком виде
    if set_version_directory(table_name, alais, data_json):
        # Устанавливаем все записи is_active в False
        with session_sync() as session:
            session.query(table_obj).update({"is_active": False})
            session.commit()

        for i_json in data_json:

            new_row = {
                "updated_at": datetime.now(),
                "is_active": True,
                "hash_address": hash_sum_256(i_json.get("id")),
                "foreign_id": i_json.get('id'),
                "text": i_json.get('text'),
            }

            with session_sync() as session:
                old_obj = session.query(table_obj).filter_by(hash_address=new_row.get("hash_address")).first()
                if old_obj is None:
                    stmt = table_obj(**new_row)
                    session.add(stmt)
                    session.commit()
                    logg.info(f"def {sys._getframe().f_code.co_name} row add: {new_row.get("hash_address")}")
                else:
                    old_obj.is_active = True
                    session.commit()
                    logg.info(
                        f"def {sys._getframe().f_code.co_name} row duplication: {new_row.get("hash_address")}")


@exception_handler(default_value="Ошибка")
def set_plan_request(new_row: dict):
    """
    Сохранение статистики запросов.
    Перезаписываем данные, если они совпали.
    """
    obj = PlanRequest
    table_name = obj.__tablename__
    obj_name = obj.__name__

    with session_scope() as session:
        # 1) Получаем список старых объектов
        obj_list = list(obj.get_all(
            {
                "dataset_checksum": new_row.get("dataset_checksum"),
                "params": new_row.get("params"),
            }
        ))
        logg.info(f'Класс {obj_name} или таблица {table_name}.')

        # 2) Меняем статус и активность
        if new_row.get("status_code") == 200 and new_row.get("count_row") >= 100_000:
            new_row["status_code"] = 206
            new_row["is_active"] = True

        # 3) Перезаписываем данные
        if obj_list:  # Проверяем, есть ли объекты для обновления
            for i_obj in obj_list:
                i_obj.is_active = new_row.get("is_active")
                i_obj.status_code = new_row.get("status_code")
                i_obj.params = new_row.get("params")
                i_obj.count_row = new_row.get("count_row")
            logg.info(f'{obj_name} обновление: {new_row}.')
        else:  # Если их нет, то добавляем их
            new_obj = obj(**new_row)  # Создаем новый экземпляр
            session.add(new_obj)  # Добавляем новый объект в сессию
            logg.info(f'{obj_name} добавление: {new_row}.')


@exception_handler(default_value="Ошибка")
def get_cmd_code(version: str) -> list:
    """
    Возвращаем список ТН ВЭД порциями по 50 экземпляров
    по запрошенной версии гармонизированной системе
    """

    batch_size = 128  # Максимальное число делителей на 2 при значении менее 100
    result = HS.get_all({"is_active": True, "classification_code": version})
    data_cmd_code = sorted([i.cmd_code for i in result])
    batch = []
    for i_r in data_cmd_code:
        batch.append(i_r)

        if len(batch) == batch_size:
            yield batch  # Возвращаем текущую порцию
            batch = []  # Очищаем порцию для следующей итерации

    # Если остались записи в последней порции, возвращаем их
    if batch:
        yield batch
