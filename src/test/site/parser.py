import httpx
import winsound
import datetime
import time
import asyncio
import json
from plyer import notification
from src.prod.site.function import df_message, is_json, split_dic, comtrade_key, rename_dic
from src.prod.site.list_error import list_error, get_key_dict37
from src.prod.site.orm import (
    OrmComtradeReturnInsert,
    OrmComtradeRequestsInsert,
    OrmComtradeRequests_Select,
    OrmComtradeRequests_Update,
    OrmComtradeRequests_Update2,
)


async def get_requests(params):  # то что передаем
    print(params)
    async with httpx.AsyncClient(event_hooks={'request': [process_request], 'response': [process_response]}) as htx:
        limits = httpx.Limits(max_keepalive_connections=4, max_connections=4)
        timeout = httpx.Timeout(15.0, read=15, connect=15)
        result: httpx.Response = await htx.get('https://comtradeapi.un.org/data/v1/get/C/A/HS',
                                               params=params,
                                               timeout=timeout,
                                               limits=limits)
        print(f'Запрос отправлен {result.status_code}')
        # if result.status_code != 200:
        #     time.sleep(12)
        #     return await get_requests(params=params)
    return result


async def process_request(request: httpx.Request):
    print(f'Статус запроса {request.method} запрос на хост {request.url}')


async def process_response(response: httpx.Response):
    status_code = response.status_code
    param_return = {"requests_id": 'obj_id',
                    "status": status_code,
                    "size": 0,
                    "elapsed_time": '-',
                    "error": '-',
                    "message": '-',
                    "resp_status": str(response.status_code),
                    "resp_elapsed": '',  # Требуется анализ
                    "resp_content_length": '-',
                    "req_date": str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}

    await response.aread()  # хуйня обязательная
    resp_text = response.text
    resp_headers = response.headers
    resp_elapsed = str(response.elapsed)
    param_return['resp_elapsed'] = resp_elapsed
    data_url = response
    if is_json(resp_text):
        data_url_json = data_url.json()
    else:
        # Перебор входящего текста и подбор к нему номера ошибки
        for item in list_error.items():
            string_part = item[1][:37]
            if string_part in resp_text:
                param_return['status'] = item[0]

    if resp_headers.get('Content-Length') is not None:
        resp_content_length = resp_headers.get('Content-Length')
        param_return['resp_content_length'] = resp_content_length

    if isinstance(data_url_json, dict):
        if data_url_json.get('error') is not None:
            error = str(data_url_json['error'])
            error = str(error).replace('"', "'")
            param_return['error'] = error

        if data_url_json.get('errorObject') is not None:
            error = str(data_url_json['error'])
            error = str(error).replace('"', "'")
            param_return['error'] = error

        if data_url_json.get('statusCode') is not None:
            status = str(data_url_json['statusCode'])
            param_return['status'] = status

        if data_url_json.get('message') is not None:
            message = str(data_url_json['message'])
            param_return['message'] = message

        if data_url_json.get('errorMessage') is not None:
            message = str(data_url_json['errorMessage'])
            param_return['message'] = message

        if data_url_json.get('elapsedTime') is not None:
            elapsed_time = str(data_url_json['elapsedTime'])
            param_return['elapsed_time'] = elapsed_time

        if data_url_json.get('count') is not None:
            size = int(data_url_json['count'])
            param_return['size'] = size
            """Необходимо проверка лимита на 2023-11-05 имеется ограничение на 100 000 строк"""

            if size >= 100_000:
                param_return['status'] = 552

        if data_url_json.get('data') is not None:
            if not data_url_json['data']:
                df = df_message('null')
                param_return['size'] = 0
    else:
        notification.notify(message='Прилетел не JSON', app_name='Comtrade', title='Неверный формат')
        data_url_text = data_url.text[:200]
        param_return['message'] = data_url_text
        cod = get_key_dict37(data_url_text, list_error)
        if cod == 'error_3':
            param_return['status'] = 429
        else:
            param_return['status'] = 'no_name'
    if param_return['status'] == 552 or param_return['size'] >= 100_000:
        pass
    # -------------------------------------------------------------------------------------------------------------
    if param_return['status'] == 200 and param_return['size'] == 0:
        # OrmComtradeRequests_Update(param_return, obj_id)
        """"Здесь можно дописать логику исключения нижестоящих запросов по периоду, чтобы изменить план запроса."""

    if param_return['status'] == 200 and 0 < param_return['size'] < 100_000:
        try:
            data_items = data_url_json['data']
            for item in data_items:
                data_dic = rename_dic(item)
                # data_dic['param_requests_id'] = obj_id
                data_dic['is_active'] = True
                print(data_dic)
                OrmComtradeReturnInsert(data_dic)
            # OrmComtradeRequests_Update(param_return, obj_id)
        except:
            print('4')
            winsound.Beep(FREQUENCY, DURATION)
            print('Ошибка записи данных!!!')
            notification.notify(message='Ошибка записи данных!!!', app_name='Comtrade', title='Экстренная остановка')
            param_return['status'] = '404'
            OrmComtradeRequests_Update2(param_return)
            exit()

    # точка выхода
    list_stop = ['429', '404', '403']
    if param_return['status'] in list_stop:
        notification.notify(message='Ошибка!!!', app_name='Comtrade', title='Экстренная остановка')
        # OrmComtradeRequests_Update(param_return, obj_id)
        print(param_return['message'])
        exit()


DURATION = 1000  # Звуковой сигнал об ошибке
FREQUENCY = 300  # Звуковой сигнал об ошибке

key_list = comtrade_key(r'../../../key.txt')

requests_plan = OrmComtradeRequests_Select()
obj_list = requests_plan[0]
obj_counter = requests_plan[1]


async def main():
    tasks = []
    for idx, obj in enumerate(obj_list):
        obj_id = obj.id
        obj_request = obj.request
        obj_request = obj_request.replace("'", '"')
        obj_json = json.loads(obj_request)
        del obj_json['typeCode']
        del obj_json['freqCode']
        obj_json['subscription-key'] = key_list[(idx + 1) % len(key_list)]

        tasks.append(get_requests(obj_json))
    await asyncio.gather(*tasks)


asyncio.run(main())
