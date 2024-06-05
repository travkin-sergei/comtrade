import datetime
import time

from src.prod.site.function import key, requestsGet, splitDic
from src.prod.site.orm import (
    ormParamReturn,
    ormParamRequestsInsert,
    ormParamRequestsSelect,
    ormParamRequestsUpdate,
)
from src.prod.system.log import logConnect

log = logConnect()

key = key(r"../../../key.txt")

log.info("Plan_request completed")
obj_counter = 1

while obj_counter > 0:
    requests_plan = ormParamRequestsSelect()
    obj_counter = 0

    for idx, obj in enumerate(requests_plan):
        obj_counter = idx
        try:
            obj_id = obj.id
            key_id = key[(idx + 1) % len(key)]

            req_time_start = datetime.datetime.now()
            param_return = {
                "requests_id": obj_id,
                "status": "0",
                "size": 0,
                "elapsed_time": "-",
                "error": "-",
                "message": "-",
                "resp_status": "-",
                "resp_elapsed": "-",
                "resp_content_length": "-",
                "req_date": str(req_time_start.strftime("%Y-%m-%d %H:%M:%S")),
                "req_time_stop": "",
            }
            obj_request = eval(obj.request)
            obj_request["typeCode"], obj_request["freqCode"] = "C", "A"
            type_code, freq_code = obj_request["typeCode"], obj_request["freqCode"]

            url = "https://comtradeapi.un.org/data/v1/get/{0}/{1}/HS".format(
                obj_request["typeCode"],
                obj_request["freqCode"]
            )
            del obj_request["typeCode"]
            del obj_request["freqCode"]
            obj_request["subscription-key"] = str(key_id)
            res = requestsGet(url, obj_request)
            print(res.url)
            del obj_request["subscription-key"]

            res_json = res.json()
            param_return["elapsed_time"] = res_json.get('elapsedTime')
            param_return["size"] = res_json.get('count') if res_json.get("data") else 0
            param_return["status"] = str(param_return.get("resp_status")) if res_json.get('statusCode') else str(
                res.status_code
            )
            param_return["status"] = "552" if param_return.get("size") >= 100_000 else param_return["status"]
            param_return["message"] = res_json.get('message') if res_json.get('message') else res_json.get(
                'errorMessage'
            )
            param_return["error"] = res_json.get('error') if res_json.get('error') else res_json.get('errorObject')
            param_return["resp_status"] = res.status_code
            param_return["resp_content_length"] = res_json.get("Content-Length")

            if param_return.get("status") == "200" and 0 < param_return.get("size") < 100_000:
                for i_item in res_json.get("data"):
                    i_item["param_requests_id"] = obj_id
                    ormParamReturn(i_item)
                    ormParamRequestsUpdate(param_return, obj_id)

            elif param_return.get("status") == "200" and param_return.get("size") == 0:
                param_return["req_time_stop"] = str(datetime.datetime.now() - req_time_start)
                ormParamRequestsUpdate(param_return, obj_id)

            elif param_return.get("status") == "552" or param_return.get("size") >= 100_000:
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

                elif len(reporter_code_split) == 1:
                    for j_code in cmd_code_split:
                        obj_request["typeCode"], obj_request["freqCode"] = type_code, freq_code
                        string_code = ",".join(j_code)

                        obj_request["cmdCode"] = string_code
                        ormParamRequestsInsert(obj_request, obj_id)
                        param_return["req_time_stop"] = str(datetime.datetime.now() - req_time_start)
                        ormParamRequestsUpdate(param_return, obj_id)
        except:
            time.sleep(240)
