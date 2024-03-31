import hashlib
import re
import sys
import json
import requests
import pandas as pd
from sqlalchemy import text

from src.prod.system.database import engine_sync
from src.prod.system.log import logConnect

log = logConnect()
MAX_RETRIES = 1  # Количество повторов запроса в случае ошибки для экономии расхода лимитированных запросов на 1 ключ


def hashSum256(*args):
    try:
        list_str = [str(i) for i in args]
        list_union = '+'.join(list_str)
        ha256 = hashlib.sha256(list_union.encode()).hexdigest()
        return ha256
    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}: {error}')


def requestsGet(link, params=None, **kwargs):
    result = None
    for _ in range(MAX_RETRIES):
        try:
            result = requests.get(link, params, **kwargs)
            result.encoding = 'utf-8'
            result.raise_for_status()
            log.info(f'def {sys._getframe().f_code.co_name}: {result.status_code}')
            break
        except Exception as error:
            log.exception(f'def {sys._getframe().f_code.co_name}: {error}')
    else:
        log.error(f'def {sys._getframe().f_code.co_name}. All retries failed')
    return result


def camelToSnake(data):
    for old_key in data:
        new_key = re.sub(r'(?<!^)(?=[A-Z])', '_', old_key).lower()
        print(old_key, ';', new_key)
    return data


def dfRename(df):
    """
    Создание единого пространства имен
    :param df:
    :return:
    """
    try:
        df = df.rename(
            columns={
                'typeCode': 'type_code',
                'freqCode': 'freq_code',
                'refPeriodId': 'ref_period_id',
                'refYear': 'ref_year',
                'refMonth': 'ref_month',
                'reporterCode': 'reporter_code',
                'reporterISO': 'reporter_iso',
                'reporterDesc': 'reporter_desc',
                'flowCode': 'flow_code',
                'flowDesc': 'flow_desc',
                'partnerCode': 'partner_code',
                'partnerISO': 'partner_iso',
                'partnerDesc': 'partner_desc',
                'partner2Code': 'partner2_code',
                'partner2ISO': 'partner2_iso',
                'partner2Desc': 'partner2_desc',
                'classificationCode': 'classification_code',
                'classificationSearchCode': 'classification_search_code',
                'isOriginalClassification': 'is_original_classification',
                'cmdCode': 'cmd_code', 'cmdDesc': 'cmd_desc',
                'aggrLevel': 'aggr_level',
                'isLeaf': 'is_leaf',
                'customsCode': 'customs_code',
                'customsDesc': 'customs_desc',
                'mosCode': 'mos_code',
                'motCode': 'mot_code',
                'motDesc': 'mot_desc',
                'qtyUnitCode': 'qty_unit_code',
                'qtyUnitAbbr': 'qty_unit_abbr',
                'isQtyEstimated': 'is_qty_estimated',
                'altQtyUnitCode': 'alt_qty_unit_code',
                'altQtyUnitAbbr': 'alt_qty_unit_abbr',
                'altQty': 'alt_qty',
                'isAltQtyEstimated': 'is_alt_qty_estimated',
                'isGrossWgtEstimated': 'is_gross_wgt_estimated',
                'isNetWgtEstimated': 'is_net_wgt_estimated',
                'netWgt': 'net_wgt',
                'grossWgt': 'gross_wgt',
                'cifvalue': 'cif_value',
                'fobvalue': 'fob_value',
                'primaryValue': 'primary_value',
                'legacyEstimationFlag': 'legacy_estimation_flag',
                'isReported': 'is_reported',
                'isAggregate': 'is_aggregate',
                'reporterNote': 'reporter_note',
                'reporterCodeIsoAlpha2': 'reporter_code_iso_alpha2',
                'reporterCodeIsoAlpha3': 'reporter_code_iso_alpha3',
                'PartnerCodeIsoAlpha2': 'partner_code_iso_alpha2',
                'PartnerCodeIsoAlpha3': 'partner_code_iso_alpha3',
                'entryEffectiveDate': 'entry_effective_date',
                'isGroup': 'is_group',
                'PartnerCode': 'partner_code',
                'PartnerDesc': 'partner_desc',
                'partnerNote': 'partner_note',
                'entryExpiredDate': 'entry_expired_date',
            }
        )
        return df
    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}: {error}')


def dfMessage(message):
    """
    Подставляет DF с необходимыми данными
    :param message:
    :return:
    """
    try:
        df = pd.DataFrame(
            {"type_code": [message], "freq_code": [message], "ref_period_id": [message], "ref_year": [message],
             "ref_month": [message], "period": [message], "reporter_code": [message], "reporter_iso": [message],
             "reporterDesc": [message], "flow_code": [message], "flow_desc": [message], "partner_code": [""],
             "partner_iso": [message], "partner_desc": [message], "partner2_code": [message],
             "partner2_iso": [message], "partner2_desc": [message], "classification_code": [message],
             "classification_search_code": [message], "is_original_classification": [message], "cmd_code": [message],
             "cmd_desc": [message], 'aggr_level': [message], "is_leaf": [message], "customs_code": [message],
             "customs_desc": [message], "mos_code": [message], "mot_code": [message], "mot_desc": [message],
             "qty_unit_code": [message], "qty_unit_abbr": [message], "qty": [message], "is_qty_estimated": [message],
             "alt_qty_unit_code": [message], "alt_qty_unit_abbr": [message], "alt_qty": [message],
             "is_alt_qty_estimated": [message], "net_wgt": [message], "is_net_wgt_estimated": [message],
             "gross_wgt": [message], "is_gross_wgt_estimated": [message], "cif_value": [""], "fob_value": [""],
             "primary_value": [""], "legacy_estimation_flag": [message], "is_reported": [message],
             "is_aggregate": [message]}
        )
        df = dfRename(df)
        return df
    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}: {error}')


def dfHashSum(df):
    try:
        """"Функция считает HashSum строк DataFrame, присоединяет ее к DataFrame"""
        hash = pd.util.hash_pandas_object(df, index=False)
        hash = pd.DataFrame({'foreign_id': hash.index, 'hash_sum': hash.values})
        hash = hash.set_index('foreign_id')
        hash = df.join(hash)
        return hash
    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}: {error}')


def updateTablComtrade():
    try:
        link = r'https://comtradeapi.un.org/files/v1/app/reference/ListofReferences.json'
        list_reference = requestsGet(link)

    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}: {error}')


def is_json(myjson):
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True


def key(address):
    """
    Загрузка ключей Comtrade. Ключи необходимо взять на сайте https://comtradeplus.un.org
    """
    with open(address) as f:
        lines = [line.rstrip() for line in f]
    return lines


def splitDic(data_dic):
    try:
        data_dic_split = data_dic.split(',')
        ret = []
        str_0 = []
        str_1 = []
        for idx, obj in enumerate(data_dic_split):
            division = idx % 2
            if division == 0:
                str_0.append(obj)
            elif division == 1:
                str_1.append(obj)
            else:
                print("Критическая ошибка разбивки данных")
                exit()
        ret.append(str_0)
        if str_1 != []:
            ret.append(str_1)
        return ret
    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}: {error}')


def sqlCountryRequest(sql_qyeru):
    try:
        with engine_sync.connect() as conn:
            res = conn.execute(text(sql_qyeru))
        return res.all()
    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}: {error}')
