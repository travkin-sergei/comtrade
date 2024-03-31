import datetime
import time
from src.prod.site.function import dfMessage, key, requestsGet, splitDic, is_json
from src.prod.site.list_error import list_error, get_key_dict37
from src.prod.site.orm import (
    ormParamReturn,
    ormParamRequestsInsert,
    ormParamRequestsSelect,
    ormParamRequestsUpdate,
)
from src.prod.system.log import logConnect

log = logConnect()

key = key(r"../../../key.txt")

url = "https://comtradeapi.un.org/data/v1/get/{}/{}/HS"

obj_counter = 1

log.info("Plan_request completed")
while obj_counter != 0:
    requests_plan = ormParamRequestsSelect()
    for idx, obj in enumerate(requests_plan):

        obj_id = obj.id
        key_id = key[(idx + 1) % len(key)]

        req_time_start = datetime.datetime.now()
        param_return = {
            "requests_id": obj_id, "status": "0", "size": 0, "elapsed_time": "-", "error": "-", "message": "-",
            "resp_status": "-", "resp_elapsed": "-", "resp_content_length": "-",
            "req_date": str(req_time_start.strftime("%Y-%m-%d %H:%M:%S")),
            "req_time_stop": "",
        }
        obj_request = eval(obj.request)
        obj_request["typeCode"], obj_request["freqCode"] = "C", "A"
        type_code, freq_code = obj_request["typeCode"], obj_request["freqCode"]

        url = url.format(obj_request["typeCode"], obj_request["freqCode"])
        del obj_request["typeCode"]
        del obj_request["freqCode"]
        obj_request["subscription-key"] = str(key_id)
        data_url = requestsGet(url, obj_request)
        del obj_request["subscription-key"]
        resp_text = data_url.text

        resp_headers = data_url.headers
        resp_status = str(data_url.status_code)
        param_return["resp_status"] = resp_status
        # until additional instructions are received from the server
        param_return["status"] = resp_status
        resp_elapsed = data_url.elapsed
        param_return["resp_elapsed"] = str(resp_elapsed)

        if is_json(resp_text):
            data_url_json = data_url.json()
        else:
            # error selection
            for item in list_error.items():
                string_part = item[1][:37]
                if string_part in resp_text:
                    param_return["status"] = item[0]

        if resp_headers.get("Content-Length") is not None:
            resp_content_length = resp_headers.get("Content-Length")
            param_return["resp_content_length"] = resp_content_length
        try:
            if isinstance(data_url_json, dict):
                if data_url_json.get("error") is not None:
                    error = str(data_url_json["error"])
                    error = str(error).replace("'", '"')
                    param_return["error"] = error

                if data_url_json.get("errorObject") is not None:
                    error = str(data_url_json["error"])
                    error = str(error).replace("'", '"')
                    param_return["error"] = error

                if data_url_json.get("statusCode") is not None:
                    status = str(data_url_json["statusCode"])
                    param_return["status"] = status

                if data_url_json.get("message") is not None:
                    message = str(data_url_json["message"])
                    param_return["message"] = message

                if data_url_json.get("errorMessage") is not None:
                    message = str(data_url_json["errorMessage"])
                    param_return["message"] = message

                if data_url_json.get("elapsedTime") is not None:
                    elapsed_time = str(data_url_json["elapsedTime"])
                    param_return["elapsed_time"] = elapsed_time

                if data_url_json.get("count") is not None:
                    size = int(data_url_json["count"])
                    param_return["size"] = size

                    if size >= 100_000:
                        param_return["status"] = "552"

                if data_url_json.get("data") is not None:
                    if not data_url_json["data"]:
                        df = dfMessage("null")
                        param_return["size"] = 0
            else:
                data_url_text = data_url.text[:200]
                param_return["message"] = data_url_text
                cod = get_key_dict37(data_url_text, list_error)
                if cod == "error_3":
                    param_return["status"] = "429"
                else:
                    param_return["status"] = "no_name"
        except ValueError as error:
            log.warning("{0}it's not json {1}".format(error, data_url))
        if param_return.get("status") == "552" or param_return.get("size") >= 100_000:
            reporter_code_split = splitDic(obj_request["reporterCode"])
            cmd_code_split = splitDic(obj_request["cmdCode"])
            if len(reporter_code_split) > 1:

                for i_code in reporter_code_split:
                    obj_request["typeCode"], obj_request["freqCode"] = type_code, freq_code
                    string_i = ",".join(i_code)

                    obj_request["reporterCode"] = string_i
                    ormParamRequestsInsert(obj_request, obj_id)
                    param_return["req_time_stop"] = str(datetime.datetime.now() - req_time_start)
                    ormParamRequestsUpdate(param_return, obj_id)
                    log.info('too much data')

            elif len(reporter_code_split) == 1:
                for j_code in cmd_code_split:
                    obj_request["typeCode"], obj_request["freqCode"] = type_code, freq_code
                    string_code = ",".join(j_code)

                    obj_request["cmdCode"] = string_code
                    ormParamRequestsInsert(obj_request, obj_id)
                    param_return["req_time_stop"] = str(datetime.datetime.now() - req_time_start)
                    ormParamRequestsUpdate(param_return, obj_id)

        if param_return["status"] == "200" and param_return["size"] == 0:
            param_return["req_time_stop"] = str(datetime.datetime.now() - req_time_start)
            ormParamRequestsUpdate(param_return, obj_id)
            log.info('no data')

        if param_return.get("status") == "200" and 0 < param_return.get("size") < 100_000:
            try:
                return_data = data_url_json["data"]
                for item in return_data:
                    item["param_requests_id"] = obj_id
                    ormParamReturn(item)

                param_return["req_time_stop"] = str(datetime.datetime.now() - req_time_start)
                ormParamRequestsUpdate(param_return, obj_id)
                log.info('the data is recorded size = {0}'.format(param_return["size"]))

            except ValueError as error:
                param_return["status"] = "404"
                param_return["req_time_stop"] = str(datetime.datetime.now() - req_time_start)
                ormParamRequestsUpdate(param_return, obj_id)
                log.critical('Data recording error! {0}'.format(error))
                time.sleep(911)

        # exit point
        list_stop = ["429", "404", "403"]
        if param_return["status"] in list_stop:
            param_return["req_time_stop"] = str(datetime.datetime.now() - req_time_start)
            ormParamRequestsUpdate(param_return, obj_id)
            log.critical('"status":"{0}","message":"{1}"'.format(param_return["status"], param_return["message"]))
            time.sleep(911)

        print(
            f'{datetime.datetime.now()};obj.id = {obj.id};status={param_return["status"]};size={param_return["size"]}')
log.info("Plan_request completed")
