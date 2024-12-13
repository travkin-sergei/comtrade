"""
Операцию сожержит описание операции с моделью данных.

Обратите внимание на hash_sum_256. Эта функция для получения адреса данных
"""

import hashlib
import json
import logging
import sys
from datetime import datetime

from .database import session_sync
from .models import (
    VersionData,
    ParamReturn,
    Code,
    PartnerAreas,
    ErrorRequest,
    TradeRegimes,
)

logging.basicConfig(
    level=logging.INFO,  # Уровень логирования
    format='%(asctime)s - %(levelname)s - %(message)s',  # Формат сообщений
    handlers=[
        logging.StreamHandler()  # Вывод в терминал
    ]
)


def hash_sum_256(*args) -> str:
    """Создание sha256. Примаем данные, приводим в нижний регистр и соединяем в строку через '+'"""

    try:
        list_str = [str(i).lower() for i in args]
        list_union = '+'.join(list_str)
        ha256 = hashlib.sha256(list_union.encode()).hexdigest()
        return ha256
    except Exception as error:
        logging.info((f'def {sys._getframe().f_code.co_name}: {error}'))


def orm_param_return(initial_json: json, dataset_checksum: int) -> None:
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
                "is_active": True,
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


def save_partner_areas(initial_json: json) -> None:
    """Добавление данных в таблицу partnerAreas."""

    # Устанавливаем все записи is_active в False
    with session_sync() as session:
        session.query(PartnerAreas).update({"is_active": False})
        session.commit()

    for i_json in initial_json.get('results'):
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
                    logging.info(f"def {sys._getframe().f_code.co_name} row duplication: {new_row.get("hash_address")}")
        except Exception as error:
            logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))


def save_version_data(incoming_data: json) -> None:
    """
    Сохранить данные в таблицу VersionData.
    Для этого необходимо иметь модель данных
    """

    for i_data in incoming_data.get('data'):
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
                stmt = None
                if old_obj is None:
                    """Если данные отсутствуют, то необходимо записать строку"""
                    stmt = VersionData(**new_row)
                elif old_obj.dataset_checksum != new_row.get("dataset_checksum"):
                    """Если данные контрольная сумма не совпадает, то обновить строку"""
                    session.query(VersionData).filter_by(
                        dataset_code=new_row.get("dataset_code"),
                    ).update(new_row)
                if stmt is not None:
                    session.add(stmt)
                session.commit()

        except Exception as error:
            logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))


def save_trade_regimes(initial_json: json) -> None:
    """
    Добавление данных в таблицу tradeRegimes.

    Ожидается, что initial_json содержит полный справочник на вход.
    """

    # Устанавливаем все записи is_active в False
    with session_sync() as session:
        session.query(TradeRegimes).update({"is_active": False})
        session.commit()

    for i_json in initial_json.get('results'):
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
                if old_obj is None:
                    stmt = TradeRegimes(**new_row)
                    session.add(stmt)
                    session.commit()
                    logging.info(f'def {sys._getframe().f_code.co_name} row add: {new_row.get("hash_address")}')
                else:
                    old_obj.is_active = True
                    session.commit()
                    logging.info(f'def {sys._getframe().f_code.co_name} row duplication: {new_row.get("hash_address")}')
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


def save_hs(incoming_data: json, i_hs: str) -> None:
    """Запись данных версии кодов ТН ВЭД."""

    for i_data in incoming_data.get('results'):
        try:
            i_data["hash_address"] = hash_sum_256(
                i_data.get('id'), i_hs
            )
            new_row = {
                "updated_at": datetime.now(),
                "is_active": True,
                "hash_address": i_data.get('hash_address'),
                "hs": i_hs,
                "cmd_code": i_data.get('id'),
                "text": i_data.get('text'),
                "parent": i_data.get('parent'),
                "is_leaf": i_data.get('isLeaf'),
                "aggr_level": i_data.get('aggrlevel'),
                "standard_unit_abbr": i_data.get('standardUnitAbbr'),
            }
            with session_sync() as session:
                old_obj = session.query(Code).filter_by(hash_address=new_row.get("hash_address")).first()
                if old_obj is None:
                    stmt = Code(**new_row)
                    session.add(stmt)
                    session.commit()

                    logging.info(f"def {sys._getframe().f_code.co_name} row add: {new_row.get("hash_address")}")
                else:
                    logging.info(f"def {sys._getframe().f_code.co_name} row duplication: {new_row.get("hash_address")}")
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
            return old_obj  # Возвращаем список записей
    except Exception as error:
        logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))
        return []  # Возвращаем пустой список в случае ошибки


def update_version_data(dataset_checksum: int) -> None:
    """Обновить версию справочника (снять отметку активности)."""

    try:
        with session_sync() as session:
            old_obj = session.query(VersionData).filter_by(dataset_checksum=dataset_checksum).first()
            if old_obj is not None:
                old_obj.is_active = False
                session.commit()
            else:
                logging.warning((f'No record found with dataset_checksum: {dataset_checksum}'))
    except Exception as error:
        logging.error((f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}'))


def save_error_request(url: str, param: dict, status_code: int, resp_code: int) -> None:
    """Сохранение данных об ошибках при запросе."""

    param.pop("subscription-key")  # перед записью ключ требуется удалить
    try:
        new_row = {
            "updated_at": datetime.now(),
            "is_active": True,
            "hash_address": hash_sum_256(url, param, str(status_code), str(resp_code)),
            "url": url,
            "param": str(param),
            "status_code": status_code,
            "resp_code": resp_code,
        }
        with session_sync() as session:
            old_obj = session.query(ErrorRequest).filter_by(hash_address=new_row.get("hash_address")).first()
            if old_obj is None:  # Добавление
                stmt = ErrorRequest(**new_row)
                session.add(stmt)
                session.commit()
                logging.info(f"def {sys._getframe().f_code.co_name} row add: {new_row.get('hash_address')}")
            else:  # Обновление
                old_obj.updated_at = datetime.now()
                old_obj.is_active = True
                session.commit()
                logging.info(f"def {sys._getframe().f_code.co_name} row update: {new_row.get('hash_address')}")
    except Exception as error:
        logging.error(f'def {sys._getframe().f_code.co_name}. The database refused to record data: {error}')
