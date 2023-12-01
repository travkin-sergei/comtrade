import numpy as np
from sqlalchemy import insert, update
from src.prod.system.models import ComtradeCmdH6, ComtradeReporter, ComtradePartner
from src.prod.system.database import engine_sync, Base, session_sync

"""Переписать на объектную модель"""


def createTable():
    """Создать таблицы в базе данных"""
    Base.metadata.create_all(engine_sync)  # Декларативные


def deactivateData(db_minus, foreign_id: str):
    """
    :param db_minus: DataFrame
    :param foreign_id: ключевое поле по которому будет происходить обновление
    :return: No data
    """
    with session_sync() as session:
        for row in db_minus[foreign_id].values.tolist():
            deactivate1 = update(db_minus).where(db_minus.c[foreign_id] == row).values(is_active=False)
            session.execute(deactivate1)
        session.commit()


def updateData(db_update, tabl_name):
    """"
    Требуется сделать нормальные переменные, чтобы код работал универсально для любой таблицы
    """
    db_update = db_update.replace({np.nan: None})
    print(db_update)
    row = db_update.to_dict('records')

    if tabl_name == 'comtrade_reporter':
        with (session_sync() as session):
            for i_row in row:
                print(i_row['foreign_id'], i_row)
                session.query(ComtradeReporter).filter_by(foreign_id=i_row['foreign_id']).update((i_row))
                session.commit()
    if tabl_name == 'comtrade_partner':
        with (session_sync() as session):
            for i_row in row:
                print(i_row['foreign_id'], i_row)
                session.query(ComtradePartner).filter_by(foreign_id=i_row['foreign_id']).update((i_row))
                session.commit()
    if tabl_name == 'comtrade_cmd_h6':
        with (session_sync() as session):
            for i_row in row:
                print(i_row['foreign_id'], i_row)
                session.query(ComtradeCmdH6).filter_by(foreign_id=i_row['foreign_id']).update((i_row))
                session.commit()
    else:
        print('Исправь функцию updateData')


def corInsertData(data: str):
    """
    Для таблицы param_requests
    :param data:
    :return:
    """
    with engine_sync.connect() as conn:
        stmt = insert('param_requests').values(parameter=data)
        max_id = conn.execute(stmt)
        conn.commit()
        return max_id.inserted_primary_key[0]
