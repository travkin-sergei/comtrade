from datetime import datetime
from src.prod.site.orm import ormParamRequestsInsert, getCountry, getCmdCode


def planRequest():
    request = {
        "typeCode": "C",
        "freqCode": "A",
        "reporterCode": '',
        "cmdCode": '',
        "flowCode": "M,X",
        "period": '',
        "maxRecords": "100000",
        "format": "JSON",
        "breakdownMode": "classic",
        "includeDesc": "True",
    }
    previous_year = datetime.now().year - 1
    year_list_str = sorted([str(i) for i in range(previous_year, previous_year - 3, -1)])
    count_country = getCountry().count()

    max_len_cmd, max_len_country, max_len = -1, 15, 0
    while max_len < 100_000:
        max_len_cmd += 1
        max_len += max_len_country * count_country / 4

    list_cmd = sorted(list(set([i_cmd.foreign_id for i_cmd in getCmdCode()])))
    list_country = sorted(list(set([str(i_country.reporter_code) for i_country in getCountry()])))

    country_chunks = sorted([list_country[i:i + max_len_country] for i in range(0, len(list_country), max_len_country)])
    cmd_chunks = sorted([list_cmd[i:i + max_len_cmd] for i in range(0, len(list_cmd), max_len_cmd)])

    for i_year in year_list_str:
        info = {"Year": i_year, }
        key = ormParamRequestsInsert(info, None)
        for i_country in country_chunks:
            country_code = ','.join(i_country)
            for i_cmd in cmd_chunks:
                cmd_code = ','.join(i_cmd)
                request["period"] = i_year
                request["cmdCode"] = cmd_code
                request["reporterCode"] = country_code
                print(request)
                ormParamRequestsInsert(request, key)



