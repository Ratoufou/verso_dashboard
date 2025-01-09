import os
import psycopg2
import pandas as pd
import json
from isodate import duration_isoformat


def RTE_Capacities_process(date, country, dir, DB_CONFIG):

    year = pd.Timestamp(date).year

    file = f'{year}_RTE_Capa_{country}.json'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        return None 
    
    with open(path, 'r') as f:
        capa_df = pd.DataFrame(json.load(f)['capacities_per_production_type']['values'])
    capa_df.start_date = pd.to_datetime(capa_df.start_date, utc=True).dt.tz_localize(None)
    capa_df.end_date = pd.to_datetime(capa_df.end_date, utc=True).dt.tz_localize(None)
    capa_df['tenor'] = (capa_df.end_date - capa_df.start_date).apply(lambda x: duration_isoformat(x))

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in capa_df.iterrows():
        cursor.execute(
            ('INSERT INTO installed_capacities ' 
            '(capa_start, capa_end, source, country, tenor, capacities_type, quantity, unit) ' 
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) '
            'ON CONFLICT (capa_start, capa_end, source, country, capacities_type) DO UPDATE SET '
            'quantity = EXCLUDED.quantity'),

            (row.start_date, 
                row.end_date, 
                'RTE', 
                country, 
                row.tenor, 
                row.type,
                round(float(row.value), 2), 
                'MW')
        )
    
    conn.commit()
    cursor.close()
    conn.close()
