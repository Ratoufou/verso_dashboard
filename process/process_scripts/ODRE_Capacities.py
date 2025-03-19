import json 
import os
import psycopg2
import pandas as pd
import datetime as dt
from isodate import duration_isoformat


def ODRE_Capacities_process(year, country, dir, DB_CONFIG):

    file = f'{year}_ODRE_Capa_{country}.json'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        return None
    
    with open(path, 'r') as f:
        data = json.load(f)

    if data['total_count'] != 1:
        raise Exception(f'The total_count is not 1 for {file}')
    if data['results'][0]['annee'] != str(year):
        raise Exception(f'Problem on the year for {file}')
    
    start = pd.Timestamp(dt.date(year, 1, 1)).tz_localize('Europe/Paris').tz_convert('UTC').tz_localize(None)
    end = pd.Timestamp(dt.date(year+1, 1, 1)).tz_localize('Europe/Paris').tz_convert('UTC').tz_localize(None)
    tenor = duration_isoformat(end - start)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    conversion_dict = {
        'parc_solaire' : 'SOLAR',
        'parc_eolien' : 'WIND'
    }

    for odre_type in conversion_dict.keys():

        cursor.execute(
            """INSERT INTO installed_capacities
            (capa_start, capa_end, source, country, tenor, capacities_type, quantity, unit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (capa_start, capa_end, source, country, capacities_type) DO NOTHING""",

            (start, 
             end, 
             'ODRE', 
             country, 
             tenor, 
             conversion_dict[odre_type], 
             round(float(data['results'][0][odre_type]), 2), 
             'MW')
        )

    conn.commit()
    cursor.close()
    conn.close()

