"""
period = MAX=12. Пример: '2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011'
reporterCode = MAX=?. Пример: '1,2,3,4,5,6,7'
cmdCode = MAX=?. Пример: '281420,280110...N'
"""
import json
import math

import numpy as np
import pandas as pd
from src.prod.site.orm import OrmParamRequests_Insert
from src.prod.system.database import engine_sync

"""
Описан вариант запросов на уровне 6 знаков за f'{}'  на все страны
"""

filter_reporter_minus = [0, 895, 896, 897, 898, ]

#filter_reporter_plus = [40, 68, 104, 144, 196, 214, 251, 328, 376, 450, 504, 554, 634, 678, 724, 792, 858]

filter_tn_ved_mimus = ['0301SS', '0302SS', '0303SS', '0304SS', '0305SS', '0306SS', '0307SS', '0308SS', '03SS', '03SSSS',
                       '1604SS', '210390SS', '250100SS', '811100SS', '811241SSSS', '811292SS', '811300SS',
                       'SS', 'SSS196S', 'SSSSSS', 'SSSSSSSS',
                       'NN', 'NNNN', 'NNNNNN',
                       'XX', 'XXXX', 'XXXXXX', 'Резерв']
# filter_tn_ved_plus = ['17', '21', '33', '63', '72', '82', '85', '96']
type_c = 'C'  # C - товары, S услуги
frequency = 'A'  # A - ежегодно, M - ежемесячно
flowCode = 'M,X'  # направление перемещения X - экспорт или M - импорт - исключил из переменных
maxRecords = '100000'  # максимальные размер выборки как и на сайте 100 000
format_data = 'JSON'  # JSON', 'TXT', 'CSV'
breakdownMode = 'classic'
includeDesc = 'True'

reporter_del = 1  # переменная, чтобы можно было изменить объем запроса
###########################################################################
period = '2020'
###########################################################################
# Коды ТН ВЭД
tn_ved_sql = """SELECT code, NTILE(200) OVER(ORDER BY code) as gr from tn_ved where is_active = true order by code"""
tn_ved = pd.read_sql_query(tn_ved_sql, con=engine_sync)
tn_ved = tn_ved[~tn_ved['code'].isin(filter_tn_ved_mimus)]  # исключаем специальные коды, которых нет в Comtrade
# tn_ved = tn_ved[tn_ved['code'].isin(filter_tn_ved_plus)]  #включить,если надо ограничить ТН ВЭД
tn_ved['code_len'] = tn_ved['code'].str.len()
tn_ved = tn_ved[tn_ved['code_len'].isin([2, 4, 6])]

# Если фильтровать коды, то надо до этого места, чтобы паспорт был верный
tn_ved = tn_ved[['gr', 'code', ]]
tn_ved = tn_ved.groupby('gr')['code'].agg(list).reset_index(name='code')
tn_ved = tn_ved.astype(str)
tn_ved['code'] = tn_ved['code'].str.replace(' ', '')
tn_ved['code'] = tn_ved['code'].str.replace('[', '')
tn_ved['code'] = tn_ved['code'].str.replace(']', '')
tn_ved['code'] = tn_ved['code'].str.replace("'", '')
###########################################################################
reporter = pd.read_sql_table('comtrade_partner', con=engine_sync)
reporter = reporter[~reporter['foreign_id'].isin(filter_reporter_minus)]
# reporter = reporter[reporter['foreign_id'].isin(filter_reporter_plus)] #включить,если надо ограничить страны
reporter = reporter.rename(columns={'foreign_id': 'reporter'})
reporter = reporter[['reporter']]
# Если фильтровать страны, то надо до этого места, чтобы паспорт был верный
reporter_list = reporter['reporter'].tolist()
reporter['C'] = np.arange(reporter.shape[0])
reporter['ostat'] = reporter['C'] % reporter_del  # страны
reporter = reporter[['reporter', 'ostat']]
reporter['reporter'] = reporter['reporter'].map(str)
reporter = reporter.groupby('ostat').reporter.agg([('count', 'count'), ('reporter', ','.join)])
reporter = reporter[['reporter']]
reporter.reset_index(drop=True, inplace=True)

###########################################################################
result = tn_ved.merge(reporter, how='cross')
result = result.sort_values(by=['reporter', 'code'])
result_json = result.to_json(orient='index')
index_result = json.loads(result_json)
# обработка результатов
reporter_list.sort()
reporter_list_size = len(reporter_list)
reporter_list = ",".join([str(i) for i in reporter_list])
reporter_size_group = math.ceil(reporter_list_size / reporter_del)

request_passport = {
    'type_c': 'Строка информационного запроса',
    'period': period,
    'reporterSize': reporter_size_group,
    'flowCode': flowCode,
    'maxRecords': maxRecords,
    'format': format_data,
    'breakdownMode': breakdownMode,
    'includeDesc': includeDesc,
}
print(request_passport)
"""Информация необходима, чтобы в дальнейшем отслеживать ошибки данных и улучшать план запросов"""

request_passport_id = OrmParamRequests_Insert(request_passport, None)  # получить паспорт

for i in index_result:
    """Здесь такие имена ключей т.к. это данные которые предназначены исключительно для сайта Comtrade"""
    request = {
        'typeCode': type_c,
        'freqCode': frequency,
        'reporterCode': index_result[i]['reporter'],
        'cmdCode': index_result[i]['code'],
        'flowCode': flowCode,
        'period': period,
        'maxRecords': maxRecords,
        'format': format_data,
        'breakdownMode': breakdownMode,
        'includeDesc': includeDesc,
    }
    typeCode_len = len(request['typeCode'])

    print(typeCode_len)
    print(request)
    print(request_passport)
    """
    Только когда поймешь как работает блок формирования данных - строки 24-49!!!
    Важно перед запросом посчитать вероятное количество возвращаемых строк в ответе,
    для этого проанализируй ранее сформированные запросы
    """
    OrmParamRequests_Insert(request, request_passport_id)
print(
    '\nЗапросов сформировано: 'f'{int(i) + 1}\n',
    'Рассматриваемый период: 'f'{period}\n',
    'Размер группы reporter: 'f'{reporter_size_group} ед.\n'
)
###########################################################################
