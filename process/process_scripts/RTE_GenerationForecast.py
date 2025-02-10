import os
import psycopg2
import pandas as pd
import json
from isodate import duration_isoformat


def RTE_GenerationForecast_process(date, country, dir, DB_CONFIG):

    file = f'{date}_RTE_GenerationForecast_{country}.json'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        return None 
    
    with open(path, 'r') as f:
        forecast_dict = json.load(f)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    for e in forecast_dict['forecasts']:

        forecast_type = e['type']
        production_type = e['production_type']

        if len(e['values']) == 0:
            continue

        df = pd.DataFrame(e['values'])
        df.start_date = pd.to_datetime(df.start_date, utc=True).dt.tz_localize(None)
        df.end_date = pd.to_datetime(df.end_date, utc=True).dt.tz_localize(None)
        df['tenor'] = (df.end_date - df.start_date).apply(lambda x: duration_isoformat(x))

        for _, row in df.iterrows():

            cursor.execute(
                """INSERT INTO generation_forecast
                (prod_start, prod_end, source, country, forecast_type, production_type, tenor, quantity, unit)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (prod_start, prod_end, source, country, forecast_type, production_type) DO NOTHING""",
                (row.start_date,
                 row.end_date,
                 'RTE',
                 country,
                 forecast_type,
                 production_type,
                 row.tenor,
                 round(float(row.value), 2),
                 'MW')
            )
    
    conn.commit()
    cursor.close()
    conn.close()