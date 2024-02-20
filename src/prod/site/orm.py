from src.prod.site.function import calculate_hash_sum
from src.prod.system.database import session_sync
from src.prod.system.models import ComtradeRequests, ComtradeReturn

"""
Когда-нибудь здесь сюда перенесем все обращения 
"""


def OrmComtradeReturnInsert(dictionary: dict):
    hash_address = (
        dictionary['type_code'],
        dictionary['freq_code'],
        dictionary['period'],
        dictionary['reporter_code'],
        dictionary['flow_code'],
        dictionary['partner_code'],
        dictionary['partner2_code'],
        dictionary['cmd_code']
    )
    print(hash_address)
    hash_data = ''


    dictionary['hash_data'] = calculate_hash_sum(dictionary)
    #dictionary['hash_address'] = calculate_hash_sum(hash_address)

    with session_sync() as session:
        try:
            session.execute(ComtradeReturn.__table__.insert(), dictionary)
            session.commit()
        except:
            print('ошибка')


def OrmComtradeRequestsInsert(dictionary: dict, id: int):
    """
    Функция для записи *********************
    :param data:
    :return: pk of inserted data
    """
    data = str(dictionary)
    with session_sync() as session:
        if id is None:
            stmt = ComtradeRequests(request=data, is_active=False, )
        else:
            stmt = ComtradeRequests(parent=id, request=data, is_active=True, )
        session.add(stmt)
        session.commit()
        session.refresh(stmt)
    return stmt.id


def OrmComtradeRequests_Select():
    """
    Обращаемся к таблице param_requests, модели DataRequests.
    Цель получить список запросов
    :return: obj & count obj
    """
    with session_sync() as session:
        obj_query = session.query(ComtradeRequests).filter(ComtradeRequests.is_active == True)
        obj_count = obj_query.count()
        session.flush()
        session.commit()
    return obj_query, obj_count


def OrmComtradeRequests_Update(dictionary: dict, id: int):
    """
    Если потребуется расширить информацию в базе данных, то
    1) передаем информацию в словарь,
    2) обрабатываем словарь
    """
    status = dictionary['status']
    size = dictionary['size']
    data = str(dictionary)
    with session_sync() as session:
        sql = session.get(ComtradeRequests, id)
        sql.response = data
        sql.status = status
        sql.is_active = False
        sql.size = size
        session.commit()
def OrmComtradeRequests_Update2(dictionary: dict):
    """
    Если потребуется расширить информацию в базе данных, то
    1) передаем информацию в словарь,
    2) обрабатываем словарь
    """
    status = dictionary['status']
    size = dictionary['size']
    data = str(dictionary)
    with session_sync() as session:
        sql = session.get(ComtradeRequests)
        sql.response = data
        sql.status = status
        sql.is_active = False
        sql.size = size
        session.commit()



def OrmComtradeRequests_UpdateError(dictionary: dict, id: int):
    data = str(dictionary)
    with session_sync() as session:
        sql = session.get(ComtradeRequests, id)
        sql.response = data
        sql.is_active = True
        session.commit()
