import os
import sys
from src.prod.site.core import createTable
from src.prod.site.function import updateTablComtrade

sys.path.insert(1, os.path.join(sys.path[0], '..'))

createTable()  # создать таблицы в базе данных

# Обновление справочников комтрейда
list_manual = [
    {'manual_id': 4, 'tabl_name': 'comtrade_partner'}, # справочник
    {'manual_id': 5, 'tabl_name': 'comtrade_reporter'},  # справочник
    {'manual_id': 20, 'tabl_name': 'comtrade_cmd_h6'}  # справочник ТН ВЭД
]
for manual in list_manual:
    updateTablComtrade(manual['manual_id'], manual['tabl_name'])
