import os
import sys

from src.prod.system.log import logConnect
from src.prod.site.plan_request import planRequest
from src.prod.site.function import requestsGet
from src.prod.site.orm import (
    ormCreateTable,
    ormPartnerAreas,
    ormReporters,
    ormCustomsCodes,
    ormDirectoryReporter,
    ormCode,
)

sys.path.insert(1, os.path.join(sys.path[0], '..'))

log = logConnect()


def main():
    try:
        ormCreateTable()

        link = r'https://comtradeapi.un.org/files/v1/app/reference/ListofReferences.json'
        list_reference = requestsGet(link)
        for i in list_reference.json()['results']:
            if i.get('fileuri').endswith('Reporters.json'):
                data = requestsGet(i.get('fileuri'))
                for j in data.json().get('results'):
                    ormDirectoryReporter(j)
            if i.get('fileuri').endswith('partnerAreas.json'):
                data = requestsGet(i.get('fileuri'))
                for j in data.json().get('results'):
                    ormPartnerAreas(j)
            if i.get('fileuri').endswith('Reporters.json'):
                data = requestsGet(i.get('fileuri'))
                for j in data.json().get('results'):
                    ormReporters(j)
            if i.get('fileuri').endswith('CustomsCodes.json'):
                data = requestsGet(i.get('fileuri'))
                for j in data.json().get('results'):
                    ormCustomsCodes(j)
            list_hs = ['H6', 'H5', 'H4', 'H3', 'H2', 'H1', 'H0', 'HS']
            for i_hs in list_hs:
                if i.get('fileuri').endswith(f'{i_hs}.json'):
                    data = requestsGet(i.get('fileuri'))
                    for j in data.json().get('results'):
                        ormCode(j, i_hs)

        planRequest()
    except Exception as error:
        log.exception(f'def {sys._getframe().f_code.co_name}. The entry point has not been passed: {error}')


# Все что ниже удалить если включать в Airflow
if __name__ == '__main__':
    main()
else:
    raise SystemExit("Это не библиотека")
