import os
import sys

from src.prod.site.function import requestsGet
from src.prod.site.orm import ormCreateTable, ormPartnerAreas, ormReporters, ormCustomsCodes, ormH6, \
    ormDirectoryReporter
from src.prod.system.log import logConnect

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
            if i.get('fileuri').endswith('H6.json'):
                data = requestsGet(i.get('fileuri'))
                for j in data.json().get('results'):
                    ormH6(j)
    # planRequest()
    except Exception as error:
        print(f'def {sys._getframe().f_code.co_name}. The entry point has not been passed: {error}')
        log.exception(f'def {sys._getframe().f_code.co_name}. The entry point has not been passed: {error}')


# Все что ниже удалить если включать в Airflow
if __name__ == '__main__':
    main()
else:
    raise SystemExit("Это не библиотека")
