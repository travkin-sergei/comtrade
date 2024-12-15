"""
Операцию сожержит описание операции с моделью данных.

Обратите внимание на hash_sum_256. Эта функция для получения адреса данных
"""

import hashlib
import json
import logging
import sys
from datetime import datetime

import requests

from .config import session_sync
from .models import (
    VersionData,
    ParamReturn,
    HsCode,
    PartnerAreas,
    ErrorRequest,
    TradeRegimes,
    HashDirectory,
)

BASE_URL = 'https://comtradeapi.un.org'


def hash_sum_256(*args) -> str:
    """Создание sha256. Примаем данные, приводим в нижний регистр и соединяем в строку через '+'"""

    try:
        list_str = [str(i).lower() for i in args]
        list_union = '+'.join(list_str)
        return hashlib.sha256(list_union.encode()).hexdigest()
    except Exception as error:
        logging.info((f'def {sys._getframe().f_code.co_name}: {error}'))
        return ''


def set_hash_directory(table_name: str, tab_list: json) -> bool:
    """Проверка. Таблицу надо обновлять?"""

    try:
        new_row = {
            "updated_at": datetime.now(),
            "is_active": True,
            "hash_address": hash_sum_256(table_name),
            "table_name": table_name,
            "tab_hash": hash_sum_256(str(tab_list)),
        }
        with session_sync() as session:
            # Получаем объект по имени таблицы
            old_obj = session.query(HashDirectory).filter_by(hash_address=new_row.get('hash_address')).first()
            if old_obj is None:  # Добавляем объект запись если его нет
                stmt = HashDirectory(**new_row)  # Убедитесь, что HsCode принимает только допустимые аргументы
                session.add(stmt)
                session.commit()
                logging.info(f'def {sys._getframe().f_code.co_name}. Write data {table_name}.')
                return True
            elif old_obj.tab_hash == new_row.get("tab_hash"):  # Если данные совпали, то ничего не меняем
                logging.info(f'def {sys._getframe().f_code.co_name}. data == data')
                return False
            else:  # Обновляем все записи, соответствующие hash_address
                session.query(HashDirectory).filter_by(
                    hash_address=new_row.get('hash_address')
                ).update({"tab_hash": new_row.get('tab_hash')})
                session.commit()
                logging.info(f'def {sys._getframe().f_code.co_name}. Add data {table_name}.')
                return True

    except Exception as error:
        logging.error(f'def {sys._getframe().f_code.co_name}. Ошибка записи в базу данных: {error}')


def add_param_return(initial_json: json, dataset_checksum: int) -> None:
    """Добавление данных в таблицу ParamReturn"""

    for i_json in initial_json.get('data'):

        try:
            i_json["hash_address"] = hash_sum_256(
                dataset_checksum,  # хеш сумма датасета по стране  (версия) от поставщика данных
                i_json.get("period"),  # дата отчета
                i_json.get("reporterCode"),  # отсчитавшаяся страна
                i_json.get("flowCode"),  # напраление перемещения
                i_json.get("partnerCode"),  # страна - торговый партнер по которой отчитываются
                i_json.get("cmdCode"),  # код ТН ВЭД
            )
            new_row = {
                "updated_at": datetime.now(),
                "is_active": False,  # переводим в True только после полной записи страны
                "hash_address": i_json.get('hash_address'),
                "type_code": i_json.get('typeCode'),
                "freq_code": i_json.get('freqCode'),
                "ref_year": i_json.get('refYear'),
                "ref_month": i_json.get('refMonth'),
                "period": i_json.get('period'),
                "reporter_code": i_json.get('reporterCode'),
                "reporter_iso": i_json.get('reporterISO'),
                # "reporter_desc": incoming_data.get('reporterDesc'),
                "flow_code": i_json.get('flowCode'),
                # "flow_desc": incoming_data.get('flowDesc'),
                "partner_code": i_json.get('partnerCode'),
                "partner_iso": i_json.get('partnerISO'),
                # "partner_desc": incoming_data.get('partnerDesc'),
                "partner2_code": i_json.get('partner2Code'),
                "partner2_iso": i_json.get('partner2ISO'),
                # "partner2_desc": incoming_data.get('partner2Desc'),
                "classification_code": i_json.get('classificationCode'),
                "classification_search_code": i_json.get('classificationSearchCode'),
                "is_original_classification": i_json.get('isOriginalClassification'),
                "cmd_code": i_json.get('cmdCode'),
                # "cmd_desc": i_json.get('cmdDesc'),
                "aggr_level": i_json.get('aggrLevel'),
                "is_leaf": i_json.get('isLeaf'),
                "customs_code": i_json.get('customsCode'),
                # "customs_desc": incoming_data.get('customsDesc'),
                "mos_code": i_json.get('mosCode'),
                "mot_code": i_json.get('motCode'),
                # "mot_desc": incoming_data.get('motDesc'),
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

            with session_sync() as session:
                old_obj = session.query(ParamReturn).filter_by(hash_address=new_row.get("hash_address")).first()
                if old_obj is None:
                    stmt = ParamReturn(**new_row)
                    session.add(stmt)
                    session.commit()
                    logging.info(f"def {sys._getframe().f_code.co_name} row add: {new_row.get("hash_address")}")
                else:
                    logging.info(f"def {sys._getframe().f_code.co_name} row duplication: {new_row.get("hash_address")}")
        except Exception as error:
            logging.error(
                f'"message" : "def {sys._getframe().f_code.co_name}. The database refuse to record data",'
                f'"error": "{error}", '
                f'"data": "{new_row}"'
            )


def set_param_return(checksum: int, is_active=False):
    """Обновляем все записи, по dataset_checksum"""

    try:
        with session_sync() as session:
            updated_obj = session.query(ParamReturn).filter_by(
                dataset_checksum=checksum
            ).update({"is_active": is_active})
            session.commit()
            logging.info(f'Updated {updated_obj} rows')
    except Exception as error:
        logging.error(f'def {sys._getframe().f_code.co_name}. The database refused to record data: {error}')


def set_partner_areas() -> None:
    """Добавление данных в таблицу partnerAreas."""

    table = 'partnerAreas'
    url = f'{BASE_URL}/files/v1/app/reference/{table}.json'
    data_json = requests.get(url, verify=False).json().get('results')

    # Проверить существует ли талица в списке в таком виде
    if set_hash_directory(table, data_json):
        # Устанавливаем все записи is_active в False
        with session_sync() as session:
            session.query(PartnerAreas).update({"is_active": False})
            session.commit()

        for i_json in data_json:
            try:
                i_json["hash_address"] = hash_sum_256(
                    i_json.get("id"),
                )
                new_row = {
                    "updated_at": datetime.now(),
                    "is_active": True,
                    "hash_address": i_json.get('hash_address'),
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
                    old_obj = session.query(PartnerAreas).filter_by(hash_address=new_row.get("hash_address")).first()
                    if old_obj is None:
                        stmt = PartnerAreas(**new_row)
                        session.add(stmt)
                        session.commit()

                        logging.info(f"def {sys._getframe().f_code.co_name} row add: {new_row.get("hash_address")}")
                    else:
                        old_obj.is_active = True
                        session.commit()
                        logging.info(
                            f"def {sys._getframe().f_code.co_name} row duplication: {new_row.get("hash_address")}")
            except Exception as error:
                logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))


def set_version_data(i_da) -> None:
    """Сохранение существующих версий данных."""

    data_json = requests.get(f'{BASE_URL}/public/v1/getDA/C/{i_da}/HS', verify=False).json().get('data')

    if set_hash_directory(i_da, data_json):
        for i_data in data_json:
            new_row = {
                "updated_at": datetime.now(),
                "is_active": True,
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
            try:
                with session_sync() as session:
                    old_obj = session.query(VersionData).filter_by(
                        dataset_code=new_row.get("dataset_code"),
                    ).first()

                    if old_obj is None:
                        # Если данные отсутствуют, записываем новую строку
                        session.add(VersionData(**new_row))
                    elif old_obj.dataset_checksum != new_row.get("dataset_checksum"):
                        # Если контрольная сумма не совпадает, обновляем строку
                        session.query(VersionData).filter_by(
                            dataset_code=new_row.get("dataset_code"),
                        ).update(new_row)
                    else:
                        pass

            except Exception as error:
                logging.error(
                    (f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))


def update_version_data(dataset_checksum: int, is_active: bool) -> None:
    """Обновить версию справочника (снять отметку активности)."""

    try:
        with session_sync() as session:
            # Обновляем все записи, соответствующие dataset_checksum
            updated_obj = session.query(VersionData).filter_by(
                dataset_checksum=dataset_checksum
            ).update({"is_active": is_active})
            session.commit()

            if updated_obj > 0:
                logging.info(f'Updated {updated_obj} rows with dataset_checksum: {dataset_checksum}.')
            else:
                logging.warning(f'No records found with dataset_checksum: {dataset_checksum}.')
    except Exception as error:
        logging.error(f'def {sys._getframe().f_code.co_name}. The database refused to record data: {error}')


def set_trade_regimes() -> None:
    """
    Добавление данных в таблицу tradeRegimes.

    Ожидается, что initial_json содержит полный справочник на вход.
    """

    # Устанавливаем все записи is_active в False

    table = 'tradeRegimes'
    url = f'{BASE_URL}/files/v1/app/reference/{table}.json'
    data_json = requests.get(url, verify=False).json().get('results')

    if set_hash_directory(table, data_json):
        with session_sync() as session:
            session.query(TradeRegimes).update({"is_active": False})
            session.commit()

        for i_json in data_json:
            try:
                i_json["hash_address"] = hash_sum_256(
                    i_json.get("id"),
                )
                new_row = {
                    "updated_at": datetime.now(),
                    "is_active": True,
                    "hash_address": i_json.get('hash_address'),
                    "flow_code": i_json.get('id'),
                    "flow_desc": i_json.get('text'),
                }
                with session_sync() as session:
                    old_obj = session.query(TradeRegimes).filter_by(hash_address=new_row.get("hash_address")).first()

                    if old_obj is None:  # Если запись отсутствует, добавляем новую
                        stmt = TradeRegimes(**new_row)
                        session.add(stmt)
                        action = "row add"
                    else:  # Если запись существует, активируем её
                        old_obj.is_active = True
                        action = "row duplication"

                    session.commit()
                    logging.info(f'def {sys._getframe().f_code.co_name} {action}: {new_row.get("hash_address")}')
            except Exception as error:
                logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))


def get_trade_regimes() -> str:
    """
    Получить список tradeRegimes.flow_code.
    :return: str
    """

    try:
        with session_sync() as session:
            flow_code = session.query(
                TradeRegimes
            ).with_entities(
                TradeRegimes.flow_code
            ).filter(TradeRegimes.is_active == True).all()

            result = [i_code for (i_code,) in flow_code]
            data = ','.join(result)

            logging.info(f'def {sys._getframe().f_code.co_name} OK')
        return data
    except Exception as error:
        logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))


def get_hs_version_data() -> list:
    """
    Получить все версии систем кодировки
    :return: list
    """

    try:
        with session_sync() as session:
            old_obj = session.query(VersionData.classification_code).filter_by(is_active=True).distinct()
            session.flush()
            session.commit()
            result = [code[0] for code in old_obj.all()]
        return result
    except Exception as error:
        logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))


def set_hs_code(i_hs) -> None:
    """Запись данных версии кодов ТН ВЭД."""

    url = f'{BASE_URL}/files/v1/app/reference/{i_hs}.json'

    incoming_data = requests.get(url, verify=False).json()
    hs_version = incoming_data.get('classCode')  # Версия кодов ТН ВЭД
    data_json = incoming_data.get('results')  # Список кодов ТН ВЭД

    if set_hash_directory(hs_version, data_json):
        with session_sync() as session:
            session.query(HsCode).filter(hs=hs_version).update({"is_active": False})
            session.commit()
        for i_data in data_json:
            try:
                i_data["hash_address"] = hash_sum_256(
                    i_data.get('id'),
                    hs_version,
                )

                new_row = {
                    "updated_at": datetime.now(),
                    "is_active": True,
                    "hash_address": i_data.get('hash_address'),
                    "hs": hs_version,
                    "cmd_code": i_data.get('id'),
                    "text": i_data.get('text'),
                    "parent": i_data.get('parent'),
                    "is_leaf": i_data.get('isLeaf'),
                    "aggr_level": i_data.get('aggrlevel'),
                    "standard_unit_abbr": i_data.get('standardUnitAbbr'),
                }
                with session_sync() as session:
                    old_obj = session.query(HsCode).filter_by(hash_address=new_row.get("hash_address")).first()
                    if old_obj is None:
                        stmt = HsCode(**new_row)
                        session.add(stmt)
                        session.commit()

                        logging.info(f"def {sys._getframe().f_code.co_name} row add: {new_row.get("hash_address")}")
                    else:
                        logging.info(
                            f"def {sys._getframe().f_code.co_name} row duplication: {new_row.get("hash_address")}"
                        )
            except Exception as error:
                logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))


def get_country_version_data() -> list:
    """
    Получить список стран из VersionData
    :return: list
    """

    try:
        with session_sync() as session:
            old_obj = session.query(
                VersionData.type_code,
                VersionData.freq_code,
                VersionData.reporter_code,
                VersionData.classification_code,
                VersionData.period,
                VersionData.dataset_checksum,
            ).filter_by(is_active=True).all()  # Используем .all() для получения всех результатов
            for i_obj in old_obj:
                yield i_obj  # Возвращаем одну запись за раз
    except Exception as error:
        logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))
        yield from []  # Возвращаем пустой список в случае ошибки


def save_error_request(checksum: int, status: int, resp_code: int) -> None:
    """Сохранение данных об ошибках при запросе."""
    try:
        hash_address = hash_sum_256(checksum, status, resp_code)
        updated_at = datetime.now()

        new_row = {
            "updated_at": updated_at,
            "is_active": True,
            "hash_address": hash_address,
            "dataset_checksum": checksum,
            "status_code": status,
            "resp_code": resp_code,
        }

        with session_sync() as session:
            old_obj = session.query(ErrorRequest).filter_by(hash_address=hash_address).first()
            if old_obj is None:
                session.add(ErrorRequest(**new_row))
                action = "add"
            else:
                old_obj.updated_at = updated_at
                old_obj.is_active = True
                action = "update"

            session.commit()
            logging.info(f"def {sys._getframe().f_code.co_name} row {action}: {hash_address}")
    except Exception as error:
        logging.error(f'def {sys._getframe().f_code.co_name}. The database refused to record data: {error}')


def get_error_request(dataset_checksum: int) -> object:
    """Запросить список dataset_checksum."""

    try:
        with session_sync() as session:
            old_obj = session.query(ErrorRequest.dataset_checksum).filter_by(
                dataset_checksum=dataset_checksum,
                is_active=True,
            ).first()
            return old_obj  # Возвращаем список записей
    except Exception as error:
        logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))
        return []  # Возвращаем пустой список в случае ошибки


def set_error_request(dataset_checksum: int, is_active: bool) -> None:
    """Обновляем все записи, соответствующие dataset_checksum"""

    try:
        with (session_sync() as session):
            updated_rows = session.query(ErrorRequest).filter_by(
                dataset_checksum=dataset_checksum
            ).update({"is_active": is_active})
            session.commit()

            if updated_rows > 0:
                logging.info(f'Updated {updated_rows} rows with dataset_checksum: {dataset_checksum}.')
            else:
                logging.warning(f'No records found with dataset_checksum: {dataset_checksum}.')
    except Exception as error:
        logging.error(f'def {sys._getframe().f_code.co_name}. The database refused to record data: {error}')
