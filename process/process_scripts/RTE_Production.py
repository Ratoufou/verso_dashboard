import os
import psycopg2
import pandas as pd
import json
from isodate import duration_isoformat


def RTE_Production_process(date, country, dir, DB_CONFIG):

    file = f'{date}_RTE_Prod_{country}.json'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        print(path)
        return None 
    
    with open(path, 'r') as f:
        prod_dict = json.load(f)

    ts_dict = dict()
    for e in prod_dict['actual_generations_per_production_type']:
        prod_type = e['production_type']

        if len(e['values']) == 0:
            continue
        df = pd.DataFrame(e['values'])
        df.start_date = pd.to_datetime(df.start_date, utc=True).dt.tz_localize(None)
        df.end_date = pd.to_datetime(df.end_date, utc=True).dt.tz_localize(None)
        df['tenor'] = (df.end_date - df.start_date).apply(lambda x: duration_isoformat(x))
        ts_dict[prod_type] = df

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for prod_type in ts_dict.keys():
        for _, row in ts_dict[prod_type].iterrows():
            cursor.execute(
                ('INSERT INTO production_per_type ' 
                '(prod_start, prod_end, source, country, tenor, production_type, quantity, unit) ' 
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) '
                'ON CONFLICT (prod_start, prod_end, source, country, production_type) DO NOTHING'),

                (row.start_date, 
                 row.end_date, 
                 'RTE', 
                 country, 
                 row.tenor, 
                 prod_type,
                 round(float(row.value), 2), 
                 'MW')
            )
    
    conn.commit()
    cursor.close()
    conn.close()
