import hashlib
import re
import sys
import json
import time

import requests
from sqlalchemy import text

from src.prod.system.database import engine_sync
from src.prod.system.log import logConnect

log = logConnect()
MAX_RETRIES = 1  # Количество повторов запроса в случае ошибки для экономии расхода лимитированных запросов на 1 ключ


def hashSum256(*args):
    try:
        list_str = [str(i).lower() for i in args]
        list_union = '+'.join(list_str)
        ha256 = hashlib.sha256(list_union.encode()).hexdigest()
        return ha256
    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}: {error}')


def requestsGet(link, params=None, **kwargs):
    result = None
    for _ in range(MAX_RETRIES):
        try:
            result = requests.get(link, params, **kwargs, verify=False)
            match result.status_code:
                case 200:
                    result.encoding = 'utf-8'
                    result.raise_for_status()
                    log.info(f'def {sys._getframe().f_code.co_name}: {result.status_code}')
                    break
                case 500:
                    log.info(f'def {sys._getframe().f_code.co_name}: {result.status_code}')
                    time.sleep(555)
                case 404:
                    log.info(f'def {sys._getframe().f_code.co_name}: {result.status_code}')
                    exit()
                case 403:
                    log.info(f'def {sys._getframe().f_code.co_name}: {result.status_code}')
                    exit()
                case _:
                    log.info(f'def {sys._getframe().f_code.co_name}: {result.status_code}')
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
