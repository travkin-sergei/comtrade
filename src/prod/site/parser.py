from plyer import notification
import winsound
import datetime

from src.prod.site.function import df_message, key, get_requests, split_dic, is_json
from src.prod.site.list_error import list_error, get_key_dict37
from src.prod.site.orm import (
    OrmParamReturn_Insert,
    OrmParamRequests_Insert,
    OrmParamRequests_Select,
    OrmParamRequests_Update,
)
from src.prod.site.function import rename_dic

DURATION = 1000  # Звуковой сигнал об ошибке
FREQUENCY = 300  # Звуковой сигнал об ошибке

key = key(r'../../../key.txt')

url = 'https://comtradeapi.un.org/data/v1/get/{}/{}/HS'

obj_counter = 1
time_start = datetime.datetime.now().replace(microsecond=0)  # для замера скорости работы
while obj_counter != 0:
    requests_plan = OrmParamRequests_Select()
    obj_list = requests_plan[0]
    obj_counter = requests_plan[1]
    for idx, obj in enumerate(obj_list):

        print(obj.id)
        obj_id = obj.id
        key_id = key[(idx + 1) % len(key)]  # требуется перебор ключей в зависимости от порядкового номера запроса
        percentage_value = "{:.00%}".format((idx + 1) / obj_counter)  # индикатор % выполнения
        req_time_start = datetime.datetime.now()
        param_return = {
            'requests_id': obj_id, 'status': '0', 'size': 0, 'elapsed_time': '-', 'error': '-', 'message': '-',
            'resp_status': '-', 'resp_elapsed': '-', 'resp_content_length': '-',
            'req_date': str(req_time_start.strftime("%Y-%m-%d %H:%M:%S")),
            'req_time_stop': '',
        }
        obj_request = eval(obj.request)
        obj_request['typeCode'], obj_request['freqCode'] = 'C', 'A'
        type_code, freq_code = obj_request['typeCode'], obj_request['freqCode']

        url = url.format(obj_request['typeCode'], obj_request['freqCode'])
        del obj_request['typeCode']
        del obj_request['freqCode']
        obj_request['subscription-key'] = str(key_id)
        data_url = get_requests(url, obj_request)
        del obj_request['subscription-key']
        resp_text = data_url.text

        resp_headers = data_url.headers
        resp_status = str(data_url.status_code)
        param_return['resp_status'] = resp_status
        param_return['status'] = resp_status  # до тех пор, пока от сервера не получены дополнительные инструкции
        resp_elapsed = data_url.elapsed
        param_return['resp_elapsed'] = str(resp_elapsed)

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
                    param_return['status'] = '552'

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
                param_return['status'] = '429'
            else:
                param_return['status'] = 'no_name'
        if param_return['status'] == '552' or param_return['size'] >= 100_000:

            reporterCode_split = split_dic(obj_request['reporterCode'])
            cmdCode_split = split_dic(obj_request['cmdCode'])

            if len(reporterCode_split) > 1:

                for i in reporterCode_split:
                    obj_request['typeCode'], obj_request['freqCode'] = type_code, freq_code
                    string_i = ','.join(i)

                    obj_request['reporterCode'] = string_i
                    OrmParamRequests_Insert(obj_request, obj_id)
                    param_return['req_time_stop'] = str(datetime.datetime.now() - req_time_start)
                    OrmParamRequests_Update(param_return, obj_id)

            elif len(reporterCode_split) == 1:
                for ii in cmdCode_split:
                    obj_request['typeCode'], obj_request['freqCode'] = type_code, freq_code
                    string_ii = ','.join(ii)

                    obj_request['cmdCode'] = string_ii
                    OrmParamRequests_Insert(obj_request, obj_id)
                    param_return['req_time_stop'] = str(datetime.datetime.now() - req_time_start)
                    OrmParamRequests_Update(param_return, obj_id)

        if param_return['status'] == '200' and param_return['size'] == 0:
            param_return['req_time_stop'] = str(datetime.datetime.now() - req_time_start)
            OrmParamRequests_Update(param_return, obj_id)
            """"
            Здесь можно дописать логику исключения нижестоящих запросов по периоду, чтобы изменить план запроса
            """
        if param_return['status'] == '200' and 0 < param_return['size'] < 100_000:
            # здесь принимаются данные, которые разрешено записывать в базу
            try:
                data_items = data_url_json['data']
                for item in data_items:
                    data_dic = rename_dic(item)
                    data_dic['param_requests_id'] = obj_id
                    data_dic['is_active'] = True
                    OrmParamReturn_Insert(data_dic)
                param_return['req_time_stop'] = str(datetime.datetime.now() - req_time_start)
                OrmParamRequests_Update(param_return, obj_id)
            except:
                winsound.Beep(FREQUENCY, DURATION)
                print('Ошибка записи данных!!!')
                notification.notify(message='Ошибка записи данных!!!', app_name='Comtrade',
                                    title='Экстренная остановка')
                param_return['status'] = '404'
                param_return['req_time_stop'] = str(datetime.datetime.now() - req_time_start)
                OrmParamRequests_Update(param_return, obj_id)
                exit()
        # точка выхода
        list_stop = ['429', '404', '403']
        if param_return['status'] in list_stop:
            notification.notify(message='Ошибка!!!', app_name='Comtrade', title='Экстренная остановка')
            param_return['req_time_stop'] = str(datetime.datetime.now() - req_time_start)
            OrmParamRequests_Update(param_return, obj_id)
            print(param_return['message'])
            exit()
        time_stop = datetime.datetime.now().replace(microsecond=0)
        data_inf = (time_stop - time_start)
        print(obj_request)
        print(
            'Выполнено 'f'{percentage_value}', 'за 'f'{data_inf}',
            'Расчетное время завершения ~ 'f'{time_start + (data_inf / (idx + 1) * obj_counter)}\n',
            '_status:', f'{param_return["status"]}', '_size:', f'{param_return["size"]}',
        )
notification.notify(message='Программа выполнена успешно', app_name='Comtrade', title='Готово')
