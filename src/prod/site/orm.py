from src.prod.system.database import session_sync
from src.prod.system.models import ParamRequests, ParamReturn

"""
Когда-нибудь здесь сюда перенесем все обращения 
"""
def OrmParamReturn_Insert(dictionary):
    """
    Функция для записи в модель InfoColumn
    :param dictionary:
    :return: pk of inserted data
    """
    with session_sync() as session:
        try:
            session.execute(ParamReturn.__table__.insert(), dictionary)
            session.commit()
        except:
            print('ошибка')


def OrmParamRequests_Insert(dictionary: dict, id: int):
    """
    Функция для записи *********************
    :param data:
    :return: pk of inserted data
    """
    data = str(dictionary)
    with session_sync() as session:
        if id is None:
            stmt = ParamRequests(request=data, is_active=False, )
        else:
            stmt = ParamRequests(parent=id, request=data, is_active=True, )

        session.add(stmt)
        session.commit()
        session.refresh(stmt)
    return stmt.id


def OrmParamRequests_Select():
    """
    Обращаемся к таблице param_requests, модели ParamRequests.
    Цель получить список запросов
    :return: obj & count obj
    """
    with session_sync() as session:
        obj_query = session.query(ParamRequests).filter(ParamRequests.is_active == True)
        obj_count = obj_query.count()
        session.flush()
        session.commit()
    return obj_query, obj_count


def OrmParamRequests_Update(dictionary: dict, id: int):
    """
    Если потребуется расширить информацию в базе данных, то
    1) передаем информацию в словарь,
    2) обрабатываем словарь
    """
    status = dictionary['status']
    size = dictionary['size']
    data = str(dictionary)
    with session_sync() as session:
        sql = session.get(ParamRequests, id)
        sql.response = data
        sql.status = status
        sql.is_active = False
        sql.size = size
        session.commit()


def OrmParamRequests_UpdateError(dictionary: dict, id: int):
    data = str(dictionary)
    with session_sync() as session:
        sql = session.get(ParamRequests, id)
        sql.response = data
        sql.is_active = True
        session.commit()
