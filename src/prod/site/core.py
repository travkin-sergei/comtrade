import sys
from sqlalchemy import update, text

from src.prod.system.database import engine_sync, session_sync
from src.prod.system.log import logConnect

log = logConnect()


def corReadyMadeTemplate():
    """param_request.py"""
    try:
        param_request_sql = """
            select id,request from comtrade.param_request as pr
            where id = (select max(id) from comtrade.param_request where parent is null)
        """
        with engine_sync.connect() as conn:
            result = conn.execute(text(param_request_sql)).all()[0]
        return result
    except Exception as error:
        log.error(f'def {sys._getframe().f_code.co_name}: {error}')


def corUpdatePlan():
    """param_request.py"""
    try:
        stmt = text("""
        update comtrade.param_request
            set is_active = True
        where parent >= (select max(id)  from comtrade.param_request where parent is null) and status = '200'
        """)
        with session_sync() as session:
            session.execute(stmt)
            session.commit()
    except Exception as error:
        log.error(f'def {sys._getframe().f_code.co_name}: {error}')


def corDeactivateData(db_minus, foreign_id: str):
    """
    :param db_minus: DataFrame
    :param foreign_id: ключевое поле по которому будет происходить обновление
    :return: No data
    """
    try:
        with session_sync() as session:
            for row in db_minus[foreign_id].values.tolist():
                deactivate1 = update(db_minus).where(db_minus.c[foreign_id] == row).values(is_active=False)
                session.execute(deactivate1)
            session.commit()
    except Exception as error:
        log.error(f'def {sys._getframe().f_code.co_name}: {error}')
