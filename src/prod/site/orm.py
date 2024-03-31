import sys

from src.prod.site.function import hashSum256
from src.prod.system.log import logConnect
from src.prod.system.database import session_sync, engine_sync
from src.prod.system.models import (
    Base,
    ParamRequests,
    ParamReturn,
    DirectoryReporter,
    PartnerAreas,
    CustomsCodes,
    Reporters,
    H6,
)

log = logConnect()


def ormCreateTable():
    """Create Table"""
    try:
        Base.metadata.create_all(engine_sync)
        # engine_sync.echo = True
    except Exception as error:
        log.error(f'def {sys._getframe().f_code.co_name}: {error}')


# -------------------------------------------- обновление справочников start -------------------------------------------
def ormDirectoryReporter(incoming_data):
    try:
        incoming_data["hash_data"] = hashSum256([i for i in incoming_data.values()])
        # it is important to match the same set of fields with the database.
        # The calculation algorithms should be the same
        incoming_data["hash_address"] = hashSum256(
            ('000' + str(incoming_data.get('reporterCode')))[-3:],
            incoming_data.get('reporterCodeIsoAlpha2'),
            incoming_data.get('reporterCodeIsoAlpha3'),
        )
        data_rec = {
            "hash_address": incoming_data.get('hash_address'),
            "hash_data": incoming_data.get('hash_data'),
            "foreign_id": incoming_data.get('id'),
            "text": incoming_data.get('text'),
            "reporter_code": ('000' + str(incoming_data.get('reporterCode')))[-3:],
            "reporter_desc": incoming_data.get('reporterDesc'),
            "reporter_note": incoming_data.get('reporterNote'),
            "reporter_code_iso_alpha2": incoming_data.get('reporterCodeIsoAlpha2'),
            "reporter_code_iso_alpha3": incoming_data.get('reporterCodeIsoAlpha3'),
            "entry_effective_date": incoming_data.get('entryEffectiveDate'),
            "is_group": incoming_data.get('isGroup'),
        }
        with session_sync() as session:
            old_obj = session.query(DirectoryReporter).filter_by(hash_address=data_rec.get("hash_address")).first()
            if old_obj is None:
                stmt = DirectoryReporter(**data_rec)
                session.add(stmt)
                session.commit()
            elif old_obj.hash_data != data_rec.get("hash_data"):
                session.query(DirectoryReporter).filter_by(hash_address=data_rec.get("hash_address")).update(data_rec)
                session.commit()
            else:
                pass
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
    finally:
        session.close()


def ormPartnerAreas(incoming_data):
    try:
        incoming_data["hash_data"] = hashSum256([i for i in incoming_data.values()])
        # it is important to match the same set of fields with the database.
        # The calculation algorithms should be the same
        incoming_data["hash_address"] = hashSum256(
            ('00' + str(incoming_data.get('PartnerCode')))[-3:],
            incoming_data.get('PartnerCodeIsoAlpha2'),
            incoming_data.get('PartnerCodeIsoAlpha3'),
        )
        data_rec = {
            "hash_address": incoming_data.get('hash_address'),
            "hash_data": incoming_data.get('hash_data'),
            "foreign_id": incoming_data.get('id'),
            "text": incoming_data.get('text'),
            "partner_code": incoming_data.get('PartnerCode'),
            "partner_desc": incoming_data.get('PartnerDesc'),
            "partner_note": incoming_data.get('partnerNote'),
            "partner_code_iso_alpha2": incoming_data.get('PartnerCodeIsoAlpha2'),
            "partner_code_iso_alpha3": incoming_data.get('PartnerCodeIsoAlpha3'),
            "entry_effective_date": incoming_data.get('entryEffectiveDate'),
            "is_group": incoming_data.get('isGroup'),
        }
        with session_sync() as session:
            old_obj = session.query(PartnerAreas).filter_by(hash_address=data_rec.get("hash_address")).first()
            if old_obj is None:
                stmt = PartnerAreas(**data_rec)
                session.add(stmt)
                session.commit()
            elif old_obj.hash_data != data_rec.get("hash_data"):
                session.query(PartnerAreas).filter_by(hash_address=data_rec.get("hash_address")).update(data_rec)
                session.commit()
            else:
                pass
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
    finally:
        session.close()


def ormReporters(incoming_data):
    try:
        incoming_data["hash_data"] = hashSum256([i for i in incoming_data.values()])
        # it is important to match the same set of fields with the database.
        # The calculation algorithms should be the same
        incoming_data["hash_address"] = hashSum256(
            ('00' + str(incoming_data.get('PartnerCode')))[-3:],
            incoming_data.get('PartnerCodeIsoAlpha2'),
            incoming_data.get('PartnerCodeIsoAlpha3'),
        )
        data_rec = {
            "hash_address": incoming_data.get('hash_address'),
            "hash_data": incoming_data.get('hash_data'),
            "foreign_id": incoming_data.get('id'),
            "text": incoming_data.get('text'),
            "reporter_code": incoming_data.get('reporterCode'),
            "reporter_desc": incoming_data.get('reporterDesc'),
            "reporter_note": incoming_data.get('reporterNote'),
            "reporter_code_iso_alpha2": incoming_data.get('reporterCodeIsoAlpha2'),
            "reporter_code_iso_alpha3": incoming_data.get('reporterCodeIsoAlpha3'),
            "entry_effective_date": incoming_data.get('entryEffectiveDate'),
            "is_group": incoming_data.get('isGroup'),
        }
        with session_sync() as session:
            old_obj = session.query(Reporters).filter_by(hash_address=data_rec.get("hash_address")).first()
            if old_obj is None:
                stmt = Reporters(**data_rec)
                session.add(stmt)
                session.commit()
            elif old_obj.hash_data != data_rec.get("hash_data"):
                session.query(Reporters).filter_by(hash_address=data_rec.get("hash_address")).update(data_rec)
                session.commit()
            else:
                pass
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
    finally:
        session.close()


def ormCustomsCodes(incoming_data):
    try:
        incoming_data["hash_data"] = hashSum256([i for i in incoming_data.values()])
        # it is important to match the same set of fields with the database.
        # The calculation algorithms should be the same
        incoming_data["hash_address"] = hashSum256(
            str(incoming_data.get('id')),
        )
        data_rec = {
            "hash_address": incoming_data.get('hash_address'),
            "hash_data": incoming_data.get('hash_data'),
            "foreign_id": incoming_data.get('id'),
            "text": incoming_data.get('text'),
        }
        with session_sync() as session:
            old_obj = session.query(CustomsCodes).filter_by(hash_address=data_rec.get("hash_address")).first()
            if old_obj is None:
                stmt = CustomsCodes(**data_rec)
                session.add(stmt)
                session.commit()
            elif old_obj.hash_data != data_rec.get("hash_data"):
                session.query(CustomsCodes).filter_by(hash_address=data_rec.get("hash_address")).update(data_rec)
                session.commit()
            else:
                pass
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
    finally:
        session.close()


def ormH6(incoming_data):
    try:
        incoming_data["hash_data"] = hashSum256([i for i in incoming_data.values()])
        # it is important to match the same set of fields with the database.
        # The calculation algorithms should be the same
        incoming_data["hash_address"] = hashSum256(
            incoming_data.get('id'),
        )
        data_rec = {
            "hash_address": incoming_data.get('hash_address'),
            "hash_data": incoming_data.get('hash_data'),
            "foreign_id": incoming_data.get('id'),
            "text": incoming_data.get('text'),
            "parent": incoming_data.get('parent'),
            "is_leaf": incoming_data.get('isLeaf'),
            "aggr_level": incoming_data.get('aggrlevel'),
            "standard_unit_abbr": incoming_data.get('standardUnitAbbr'),
        }
        with session_sync() as session:
            old_obj = session.query(H6).filter_by(hash_address=data_rec.get("hash_address")).first()
            if old_obj is None:
                stmt = H6(**data_rec)
                session.add(stmt)
                session.commit()
            elif old_obj.hash_data != data_rec.get("hash_data"):
                session.query(H6).filter_by(hash_address=data_rec.get("hash_address")).update(data_rec)
                session.commit()
            else:
                pass
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
    finally:
        session.close()


# -------------------------------------------- обновление справочников stop -------------------------------------------
def ormParamReturn(incoming_data):
    """
    add or update objects class ParamReturn
    """
    try:
        incoming_data["hash_data"] = hashSum256([i for i in incoming_data.values()])
        # it is important to match the same set of fields with the database.
        # The calculation algorithms should be the same
        incoming_data["hash_address"] = hashSum256(
            incoming_data["typeCode"], incoming_data["freqCode"], incoming_data["period"],
            incoming_data["reporterCode"],
            incoming_data["flowCode"], incoming_data["partnerCode"], incoming_data["partner2Code"],
            incoming_data["cmdCode"],
        )
        data_rec = {
            "is_active": True,
            "hash_address": incoming_data.get('hash_address'),
            "hash_data": incoming_data.get('hash_data'),
            "param_requests_id": incoming_data.get('param_requests_id'),
            "type_code": incoming_data.get('typeCode'),
            "freq_code": incoming_data.get('freqCode'),
            "ref_period_id": incoming_data.get('refPeriodId'),
            "ref_year": incoming_data.get('refYear'),
            "ref_month": incoming_data.get('refMonth'),
            "period": incoming_data.get('period'),
            "reporter_code": incoming_data.get('reporterCode'),
            "reporter_iso": incoming_data.get('reporterISO'),
            "reporter_desc": incoming_data.get('reporterDesc'),
            "flow_code": incoming_data.get('flowCode'),
            "flow_desc": incoming_data.get('flowDesc'),
            "partner_code": incoming_data.get('partnerCode'),
            "partner_iso": incoming_data.get('partnerISO'),
            "partner_desc": incoming_data.get('partnerDesc'),
            "partner2_code": incoming_data.get('partner2Code'),
            "partner2_iso": incoming_data.get('partner2ISO'),
            "partner2_desc": incoming_data.get('partner2Desc'),
            "classification_code": incoming_data.get('classificationCode'),
            "classification_search_code": incoming_data.get('classificationSearchCode'),
            "is_original_classification": incoming_data.get('isOriginalClassification'),
            "cmd_code": incoming_data.get('cmdCode'),
            "cmd_desc": incoming_data.get('cmdDesc'),
            "aggr_level": incoming_data.get('aggrLevel'),
            "is_leaf": incoming_data.get('isLeaf'),
            "customs_code": incoming_data.get('customsCode'),
            "customs_desc": incoming_data.get('customsDesc'),
            "mos_code": incoming_data.get('mosCode'),
            "mot_code": incoming_data.get('motCode'),
            "mot_desc": incoming_data.get('motDesc'),
            "qty_unit_code": incoming_data.get('qtyUnitCode'),
            "qty_unit_abbr": incoming_data.get('qtyUnitAbbr'),
            "qty": incoming_data.get('qty'),
            "is_qty_estimated": incoming_data.get('isQtyEstimated'),
            "alt_qty_unit_code": incoming_data.get('altQtyUnitCode'),
            "alt_qty_unit_abbr": incoming_data.get('altQtyUnitAbbr'),
            "alt_qty": incoming_data.get('altQty'),
            "is_alt_qty_estimated": incoming_data.get('isAltQtyEstimated'),
            "net_wgt": incoming_data.get('netWgt'),
            "is_net_wgt_estimated": incoming_data.get('isNetWgtEstimated'),
            "gross_wgt": incoming_data.get('grossWgt'),
            "is_gross_wgt_estimated": incoming_data.get('isGrossWgtEstimated'),
            "cif_value": incoming_data.get('cifvalue'),
            "fob_value": incoming_data.get('fobvalue'),
            "primary_value": incoming_data.get('primaryValue'),
            "legacy_estimation_flag": incoming_data.get('legacyEstimationFlag'),
            "is_reported": incoming_data.get('isReported'),
            "is_aggregate": incoming_data.get('isAggregate'),
        }
        with session_sync() as session:
            old_obj = session.query(ParamReturn).filter_by(hash_address=data_rec.get("hash_address")).first()
            if old_obj is None:
                stmt = ParamReturn(**data_rec)
                session.add(stmt)
                session.commit()
            elif old_obj.hash_data != data_rec.get("hash_data"):
                session.query(ParamReturn).filter_by(hash_address=data_rec.get("hash_address")).update(data_rec)
                session.commit()
            else:
                pass
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
    finally:
        session.close()


def ormParamRequestsInsert(dictionary: dict, id: int):
    """
    :param data:
    :return: pk of inserted data
    """
    try:
        new_obj = ParamRequests(
            id=id,
            request=dictionary,
        )
        with session_sync() as session:
            if new_obj.id:
                stmt = ParamRequests(parent=new_obj.id, request=new_obj.request, is_active=True)
            else:
                stmt = ParamRequests(request=new_obj.request, is_active=False)
            session.add(stmt)
            session.commit()
            session.refresh(stmt)
        return stmt.id
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
    finally:
        session.close()


def ormParamRequestsSelect():
    """
    Цель получить список запросов
    :return: obj
    """
    try:
        with session_sync() as session:
            obj_query = session.query(ParamRequests).filter_by(is_active=True)
            session.flush()
            session.commit()
        return obj_query
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
    finally:
        session.close()


def ormParamRequestsUpdate(dictionary: dict, object_id: int):
    try:
        new_obj = ParamRequests(
            status=dictionary.get('status'),
            size=dictionary.get('size'),
            response=str(dictionary),
        )
        with session_sync() as session:
            old_obj = session.get(ParamRequests, object_id)
            old_obj.status = new_obj.status
            old_obj.size = new_obj.size
            old_obj.response = new_obj.response
            session.commit()
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The database refuse to record data: {error}')
    finally:
        session.close()
