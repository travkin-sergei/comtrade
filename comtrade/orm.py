"""
Операции с моделью данных.
"""

import hashlib
import json
import sys

from comtrade.database import session_sync
from comtrade.log import logConnect
from comtrade.models import (
    VersionData,
    ParamReturn,
    Code,
    PartnerAreas,
)

log = logConnect()


def hash_sum_256(*args):
    try:
        list_str = [str(i).lower() for i in args]
        list_union = '+'.join(list_str)
        ha256 = hashlib.sha256(list_union.encode()).hexdigest()
        return ha256
    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}: {error}')


def orm_param_return(initial_json: json, dataset_checksum: int) -> None:
    """
    Добавление данных в таблицу ParamReturn
    """
    for i_json in initial_json.get('data'):

        try:
            i_json["hash_address"] = hash_sum_256(
                i_json["typeCode"],
                i_json["freqCode"],
                i_json["period"],
                i_json["reporterCode"],
                i_json["flowCode"],
                i_json["partnerCode"],
                i_json["cmdCode"],
            )
            new_row = {
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
        except Exception as error:
            print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
            log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')


def save_partner_areas(initial_json: json) -> None:
    """
    Добавление данных в таблицу partnerAreas.
    """
    for i_json in initial_json.get('results'):
        try:
            i_json["hash_address"] = hash_sum_256(
                i_json["id"],
            )
            new_row = {
                "hash_address": i_json.get('hash_address'),
                "is_active": True,
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
        except Exception as error:
            print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
            log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')


def save_version_data(incoming_data: json) -> None:
    """
    Сохранить данные в таблицу VersionData.
    Для этого необходимо иметь модель данных
    """
    for i_data in incoming_data.get('data'):
        new_row = {
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
            print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
            log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')


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
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')


def save_hs(incoming_data: json, i_hs: str) -> None:
    """Запись данных версии кодов ТН ВЭД."""
    for i_data in incoming_data.get('results'):
        try:
            i_data["hash_address"] = hash_sum_256(i_data.get('id'), i_hs)
            new_data = {
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
                old_obj = session.query(Code).filter_by(hash_address=new_data.get("hash_address")).first()
                if old_obj is None:
                    stmt = Code(**new_data)
                    session.add(stmt)
                    session.commit()
        except Exception as error:
            print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
            log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')


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
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
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
                print(f'No record found with dataset_checksum: {dataset_checksum}')
                log.warning(f'No record found with dataset_checksum: {dataset_checksum}')
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
