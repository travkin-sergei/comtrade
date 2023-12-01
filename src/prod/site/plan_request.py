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
type_c = 'C'  # C - товары, S услуги
frequency = 'A'  # A - ежегодно, M - ежемесячно
flowCode = 'M,X'  # направление перемещения X - экспорт или M - импорт - исключил из переменных
maxRecords = '100000'  # максимальные размер выборки как и на сайте 100 000
format_data = 'JSON'  # JSON', 'TXT', 'CSV'
breakdownMode = 'classic'
includeDesc = 'True'

reporter_del = 20  # переменная, чтобы можно было изменить объем запроса
cmd_h6_del = 40  # переменная, чтобы можно было изменить объем запроса
###########################################################################
period = '2022'
###########################################################################
# Коды ТН ВЭД
cmd_h6 = pd.read_sql_table('comtrade_cmd_h6', con=engine_sync)


cmd_h6['len'] = cmd_h6['foreign_id'].str.len()
cmd_h6 = cmd_h6.rename(columns={'foreign_id': 'code'})
cmd_h6 = cmd_h6[cmd_h6['len'] == 6]  # длинна знаков
cmd_h6 = cmd_h6[['code']]


# Если фильтровать коды, то надо до этого места, чтобы паспорт был верный
cmd_h6_list = cmd_h6['code'].tolist()
cmd_h6['C'] = np.arange(cmd_h6.shape[0])
cmd_h6['ostat'] = cmd_h6['C'] % cmd_h6_del  # ТН ВЭД
cmd_h6 = cmd_h6[['code', 'ostat']]
cmd_h6['code'] = cmd_h6['code'].map(str)
cmd_h6 = cmd_h6.groupby('ostat').code.agg([('count', 'count'), ('code', ','.join)])
cmd_h6 = cmd_h6[['code']]
cmd_h6.reset_index(drop=True, inplace=True)

###########################################################################
reporter = pd.read_sql_table('comtrade_partner', con=engine_sync)

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
result = cmd_h6.merge(reporter, how='cross')
result_json = result.to_json(orient='index')
index_result = json.loads(result_json)

# обработка результатов
reporter_list.sort()
reporter_list_size = len(reporter_list)
reporter_list = ",".join([str(i) for i in reporter_list])
reporter_size_group = math.ceil(reporter_list_size / reporter_del)

cmd_h6_list.sort()
cmd_h6_list_size = len(cmd_h6_list)
cmd_h6_list = ",".join([str(i) for i in cmd_h6_list])
cmd_h6_size_group = math.ceil(cmd_h6_list_size / cmd_h6_del)

request_passport = {
    'type_c': 'Строка информационного запроса',
    'period': period,
    'reporterSize': reporter_size_group,
    'cmdCodeSize': cmd_h6_size_group,
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
        'Размер группы reporter: 'f'{reporter_size_group} ед.\n',
        'Размер группы cmd_h6: 'f'{cmd_h6_size_group} ед.\n'
    )
###########################################################################
