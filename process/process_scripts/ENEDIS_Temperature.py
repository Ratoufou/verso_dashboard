from isodate import parse_duration, duration_isoformat
import psycopg2
import pandas as pd
import os
import json
import numpy as np


def ENEDIS_Temperature_process(date, country, dir, DB_CONFIG):

    file = f'{date}_ENEDIS_Temp_{country}.json'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        return 
    
    with open(path, 'r') as f:
        temp_dict = json.load(f)

    if temp_dict['total_count'] == 0:
        return
    
    df = pd.DataFrame(temp_dict['results']).rename(
        columns={
            'temperature_normale_lissee_degc' : 'tn',
            'temperature_realisee_lissee_degc' : 'tr'
        }
    ).sort_values(by='horodate')
    df['start'] = pd.to_datetime(df.horodate).dt.tz_convert('UTC').dt.tz_localize(None)
    df['trs'] = np.where(df['tr'] < 15, df['tr'], df['tn'])
    df['delta_t'] = df['tr'] - df['tn']
    df['delta_ts'] = df['trs'] - df['tn']

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    timedelta = df['start'][1] - df['start'][0]
    tenor = duration_isoformat(timedelta)
    df['end'] = df['start'] + timedelta

    for _, row in df.iterrows():
        cursor.execute(
            """INSERT INTO temperature
            (temp_start, temp_end, source, country, tenor, tn, tr, trs, delta_t, delta_ts, unit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (temp_start, temp_end, source, country) DO NOTHING""",

            (row.start,
             row.end,
             'ENEDIS_API',
             country,
             tenor,
             row.tn,
             row.tr,
             row.trs,
             row.delta_t,
             row.delta_ts,
             '°C')
        )
        
    conn.commit()
    cursor.close()
    conn.close()
