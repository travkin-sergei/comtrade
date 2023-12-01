import hashlib
from array import array

import requests
import pandas as pd
from src.prod.site.core import deactivateData, updateData
from src.prod.system.database import engine_sync

def calculate_hash_sum(list: list):
    # Предполагается, что сравнить хеш сумму можно будет до вставки в базу данных
    """
    Требуется для расчета hash_address и hash_data. Алгоритм sha256
    :param list: list
    :return: list
    """
    list_str = [str(i) for i in list]
    list_union = '+'.join(list_str)
    ha256 = hashlib.sha256(list_union.encode()).hexdigest()
    return ha256


def rename_dic(dictionary):
    dictionary['type_code'] = dictionary['typeCode']
    del dictionary['typeCode']
    dictionary['freq_code'] = dictionary['freqCode']
    del dictionary['freqCode']
    dictionary['ref_period_id'] = dictionary['refPeriodId']
    del dictionary['refPeriodId']
    dictionary['ref_year'] = dictionary['refYear']
    del dictionary['refYear']
    dictionary['ref_month'] = dictionary['refMonth']
    del dictionary['refMonth']
    dictionary['reporter_code'] = dictionary['reporterCode']
    del dictionary['reporterCode']
    dictionary['reporter_iso'] = dictionary['reporterISO']
    del dictionary['reporterISO']
    dictionary['reporter_desc'] = dictionary['reporterDesc']
    del dictionary['reporterDesc']
    dictionary['flow_code'] = dictionary['flowCode']
    del dictionary['flowCode']
    dictionary['flow_desc'] = dictionary['flowDesc']
    del dictionary['flowDesc']
    dictionary['partner_code'] = dictionary['partnerCode']
    del dictionary['partnerCode']
    dictionary['partner_iso'] = dictionary['partnerISO']
    del dictionary['partnerISO']
    dictionary['partner_desc'] = dictionary['partnerDesc']
    del dictionary['partnerDesc']
    dictionary['partner2_code'] = dictionary['partner2Code']
    del dictionary['partner2Code']
    dictionary['partner2_iso'] = dictionary['partner2ISO']
    del dictionary['partner2ISO']
    dictionary['partner2_desc'] = dictionary['partner2Desc']
    del dictionary['partner2Desc']
    dictionary['classification_code'] = dictionary['classificationCode']
    del dictionary['classificationCode']
    dictionary['classification_search_code'] = dictionary['classificationSearchCode']
    del dictionary['classificationSearchCode']
    dictionary['is_original_classification'] = dictionary['isOriginalClassification']
    del dictionary['isOriginalClassification']
    dictionary['aggr_level'] = dictionary['aggrLevel']
    del dictionary['aggrLevel']
    dictionary['cmd_code'] = dictionary['cmdCode']
    del dictionary['cmdCode']
    dictionary['cmd_desc'] = dictionary['cmdDesc']
    del dictionary['cmdDesc']
    dictionary['is_leaf'] = dictionary['isLeaf']
    del dictionary['isLeaf']
    dictionary['customs_code'] = dictionary['customsCode']
    del dictionary['customsCode']
    dictionary['customs_desc'] = dictionary['customsDesc']
    del dictionary['customsDesc']
    dictionary['mos_code'] = dictionary['mosCode']
    del dictionary['mosCode']
    dictionary['mot_code'] = dictionary['motCode']
    del dictionary['motCode']
    dictionary['mot_desc'] = dictionary['motDesc']
    del dictionary['motDesc']
    dictionary['qty_unit_code'] = dictionary['qtyUnitCode']
    del dictionary['qtyUnitCode']
    dictionary['qty_unit_abbr'] = dictionary['qtyUnitAbbr']
    del dictionary['qtyUnitAbbr']
    dictionary['is_qty_estimated'] = dictionary['isQtyEstimated']
    del dictionary['isQtyEstimated']
    dictionary['alt_qty_unit_code'] = dictionary['altQtyUnitCode']
    del dictionary['altQtyUnitCode']
    dictionary['alt_qty_unit_abbr'] = dictionary['altQtyUnitAbbr']
    del dictionary['altQtyUnitAbbr']
    dictionary['alt_qty'] = dictionary['altQty']
    del dictionary['altQty']
    dictionary['is_alt_qty_estimated'] = dictionary['isAltQtyEstimated']
    del dictionary['isAltQtyEstimated']
    dictionary['net_wgt'] = dictionary['netWgt']
    del dictionary['netWgt']
    dictionary['is_net_wgt_estimated'] = dictionary['isNetWgtEstimated']
    del dictionary['isNetWgtEstimated']
    dictionary['gross_wgt'] = dictionary['grossWgt']
    del dictionary['grossWgt']
    dictionary['is_gross_wgt_estimated'] = dictionary['isGrossWgtEstimated']
    del dictionary['isGrossWgtEstimated']
    dictionary['cif_value'] = dictionary['cifvalue']
    del dictionary['cifvalue']
    dictionary['fob_value'] = dictionary['fobvalue']
    del dictionary['fobvalue']
    dictionary['primary_value'] = dictionary['primaryValue']
    del dictionary['primaryValue']
    dictionary['legacy_estimation_flag'] = dictionary['legacyEstimationFlag']
    del dictionary['legacyEstimationFlag']
    dictionary['is_reported'] = dictionary['isReported']
    del dictionary['isReported']
    dictionary['is_aggregate'] = dictionary['isAggregate']
    del dictionary['isAggregate']
    return dictionary
""""
Здась хранится информация по унификации имен и обработка справочников из Comtrade
"""
c


def df_rename(df):
    """
    Создание единого пространства имен
    :param df:
    :return:
    """
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


def df_message(message):
    """
    Подставляет DF с необходимыми данными
    :param message:
    :return:
    """
    df = pd.DataFrame(
        {'type_code': [message], 'freq_code': [message], 'ref_period_id': [message], 'ref_year': [message],
         'ref_month': [message], 'period': [message], 'reporter_code': [message], 'reporter_iso': [message],
         'reporterDesc': [message], 'flow_code': [message], 'flow_desc': [message], 'partner_code': [''],
         'partner_iso': [message], 'partner_desc': [message], 'partner2_code': [message],
         'partner2_iso': [message], 'partner2_desc': [message], 'classification_code': [message],
         'classification_search_code': [message], 'is_original_classification': [message], 'cmd_code': [message],
         'cmd_desc': [message], 'aggr_level': [message], 'is_leaf': [message], 'customs_code': [message],
         'customs_desc': [message], 'mos_code': [message], 'mot_code': [message], 'mot_desc': [message],
         'qty_unit_code': [message], 'qty_unit_abbr': [message], 'qty': [message], 'is_qty_estimated': [message],
         'alt_qty_unit_code': [message], 'alt_qty_unit_abbr': [message], 'alt_qty': [message],
         'is_alt_qty_estimated': [message], 'net_wgt': [message], 'is_net_wgt_estimated': [message],
         'gross_wgt': [message], 'is_gross_wgt_estimated': [message], 'cif_value': [''], 'fob_value': [''],
         'primary_value': [''], 'legacy_estimation_flag': [message], 'is_reported': [message],
         'is_aggregate': [message]}
    )
    df = df_rename(df)
    return df


def df_hash_sum(df):
    """"Функция считает HashSum строк DataFrame, присоединяет ее к DataFrame"""
    hash = pd.util.hash_pandas_object(df, index=False)
    hash = pd.DataFrame({'foreign_id': hash.index, 'hash_sum': hash.values})
    hash = hash.set_index('foreign_id')
    hash = df.join(hash)
    return hash


def updateTablComtrade(number, tabl_name):
    # Получаем данные из Comtrade
    link = r'https://comtradeapi.un.org/files/v1/app/reference/ListofReferences.json'
    comtrade_json = requests.get(requests.get(link).json()['results'][number]['fileuri']).json()['results']
    external = pd.DataFrame.from_dict(comtrade_json, orient='columns')
    external = df_rename(external)
    external = external.rename(columns={'id': 'foreign_id'})
    external['is_active'] = True
    local = pd.read_sql_table(tabl_name, con=engine_sync)
    local = local[external.columns]  # оставить только те столбцы, которые имется в загружаемых данных
    ext = df_hash_sum(external)
    loc = df_hash_sum(local)

    ext_res = ext[~ext['hash_sum'].isin(loc['hash_sum'])]
    loc_res = loc[~loc['hash_sum'].isin(ext['hash_sum'])]
    db_plus = ext_res[~ext_res['foreign_id'].isin(loc_res['foreign_id'])]
    db_plus = db_plus[external.columns]
    db_minus = loc_res[~loc_res['foreign_id'].isin(ext_res['foreign_id'])]
    db_minus = db_minus[external.columns]
    # db_update = loc_res[loc_res['foreign_id'].isin(ext_res['foreign_id'])] # проверить логику
    db_update = ext_res[ext_res['foreign_id'].isin(loc_res['foreign_id'])]
    db_update = db_update[external.columns]

    if not db_plus.empty:
        print('В таблице ' + f'{tabl_name}' + ' обнаружены записи, которые требуется добавить')
        print(db_plus.info())
        db_plus.to_sql(tabl_name, con=engine_sync, if_exists='append', index=False)
        print('Записи добавлены')
    if not db_minus.empty:
        print('В таблице ' + f'{tabl_name}' + ' обнаружены записи, которые требуется деактивировать')
        deactivateData(db_minus, 'foreign_id')
        print('Записи деактивированы')
    if not db_update.empty:
        print('В таблице ' + f'{tabl_name}' + ' обнаружены записи, которые требуется обновить')
        print(db_update.info())
        updateData(db_update, tabl_name)
        print('Записи обновлены')