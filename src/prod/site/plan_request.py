import json
import math
import numpy as np
import pandas as pd
from datetime import datetime
from src.prod.site.core import corUpdatePlan, corReadyMadeTemplate
from src.prod.site.orm import ormParamRequestsInsert
from src.prod.system.database import engine_sync
from src.prod.system.log import logConnect

"""
period = MAX=12. example: "2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011"
reporterCode = MAX=?. example: "1,2,3,4,5,6,7"
cmdCode = MAX=?. example: "281420,280110...N"
"""

log = logConnect()


def planRequest():
    """
    creating a query plan
    """
    previous_year = datetime.now().year - 1
    year_list_str = ",".join([str(i) for i in range(previous_year, previous_year - 3, -1)])

    result = corReadyMadeTemplate()
    print(f'result {result}')
    request_plan_json = json.loads(result[1].replace("'", '"'))

    if year_list_str == request_plan_json.get("period"):
        corUpdatePlan()

    else:
        filter_reporter_minus = [0, 895, 896, 897, 898, ]  # They are not countries

        type_c = "C"
        frequency = "A"
        flow_сode = "M,X"
        max_records = "100000"
        format_data = "JSON"
        breakdownMode = "classic"
        includeDesc = "True"

        reporter_del = 1
        ###########################################################################
        tn_ved_sql = """
        /* Comtradeapi.un.org accepts only numbers */
        SELECT
            code
            , NTILE(200) OVER(ORDER BY code) as gr
        FROM comtrade.tn_ved
        WHERE is_active = true
            AND REGEXP_LIKE(code, "[0-9]+$")
        ORDER BY  code
        """
        tn_ved = pd.read_sql_query(tn_ved_sql, con=engine_sync)

        if tn_ved.empty:
            log.critical('df tn_ved is empty!')
            exit()
        tn_ved["code_len"] = tn_ved["code"].str.len()
        # the length of the HS code
        tn_ved = tn_ved[tn_ved["code_len"].isin([2, 4, 6])]

        tn_ved = tn_ved[["gr", "code", ]]
        tn_ved = tn_ved.groupby("gr")["code"].agg(list).reset_index(name="code")
        tn_ved = tn_ved.astype(str)
        tn_ved["code"] = tn_ved["code"].str.replace(" ", "")
        tn_ved["code"] = tn_ved["code"].str.replace("[", "")
        tn_ved["code"] = tn_ved["code"].str.replace("]", "")
        tn_ved["code"] = tn_ved["code"].str.replace("'", "")
        ###########################################################################

        reporter = pd.read_sql_table("comtrade_partner", con=engine_sync, schema="comtrade")
        if reporter.empty:
            log.critical('df reporter is empty!')
            exit()
        reporter = reporter[~reporter["foreign_id"].isin(filter_reporter_minus)]
        reporter = reporter.rename(columns={"foreign_id": "reporter"})
        reporter = reporter[["reporter"]]
        reporter_list = reporter["reporter"].tolist()
        reporter["C"] = np.arange(reporter.shape[0])
        reporter["ostat"] = reporter["C"] % reporter_del
        reporter = reporter[["reporter", "ostat"]]
        reporter["reporter"] = reporter["reporter"].map(str)
        reporter = reporter.groupby("ostat").reporter.agg([("count", "count"), ("reporter", ",".join)])
        reporter = reporter[["reporter"]]
        reporter.reset_index(drop=True, inplace=True)

        ###########################################################################
        result = tn_ved.merge(reporter, how="cross")
        result = result.sort_values(by=["reporter", "code"])
        result_json = result.to_json(orient="index")
        index_result = json.loads(result_json)

        reporter_list.sort()
        reporter_list_size = len(reporter_list)
        reporter_size_group = math.ceil(reporter_list_size / reporter_del)

        # for query plan statistics
        request_passport = {
            "type_c": "Строка информационного запроса",
            "period": year_list_str,
            "reporterSize": reporter_size_group,
            "flowCode": flow_сode,
            "maxRecords": max_records,
            "format": format_data,
            "breakdownMode": breakdownMode,
            "includeDesc": includeDesc,
        }

        request_passport_id = ormParamRequestsInsert(request_passport, None)

        for i in index_result:
            request = {
                "typeCode": type_c,
                "freqCode": frequency,
                "reporterCode": index_result[i]["reporter"],
                "cmdCode": index_result[i]["code"],
                "flowCode": flow_сode,
                "period": year_list_str,
                "maxRecords": max_records,
                "format": format_data,
                "breakdownMode": breakdownMode,
                "includeDesc": includeDesc,
            }
            ormParamRequestsInsert(request, request_passport_id)


planRequest()
