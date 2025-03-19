import os
import psycopg2
import pandas as pd
import json
from isodate import duration_isoformat
import datetime as dt


def RTE_Production_process(date, country, dir, DB_CONFIG):

    file = f'{date}_RTE_Prod_{country}.json'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        return None 
    
    with open(path, 'r') as f:
        prod_dict = json.load(f)

    ts_dict = dict()
    for e in prod_dict['actual_generations_per_production_type']:
        prod_type = e['production_type']
        start = pd.Timestamp(e['start_date'], tz='UTC').tz_localize(None)   
        end = pd.Timestamp(e['end_date'], tz='UTC').tz_localize(None)  
        time_range = pd.date_range(start=start, end=end, freq='h', inclusive='left')

        if len(e['values']) == 0:
            continue

        df = pd.DataFrame(e['values'])
        df.start_date = pd.to_datetime(df.start_date, utc=True).dt.tz_localize(None)
        df.end_date = pd.to_datetime(df.end_date, utc=True).dt.tz_localize(None)

        df['timedelta'] = df.end_date - df.start_date
        timedeltas = df.timedelta.unique()
        if (len(timedeltas) != 1) or (timedeltas[0] != pd.Timedelta(hours=1)):
            raise Exception(f'An interval does not last an hour for {date} - {prod_type} : {list(timedeltas)}')
        
        df.drop_duplicates(subset=['start_date'], inplace=True)
        reindexed_df = df.set_index('start_date').reindex(time_range)
        missing_mask = reindexed_df.isna().any(axis=1)
        reindexed_df.loc[missing_mask, 'flag'] = 'Interpolated'
        reindexed_df.loc[~missing_mask, 'flag'] = 'Unchanged'
        reindexed_df.value = reindexed_df.value.interpolate().bfill().ffill()
        reindexed_df.loc[missing_mask, 'end_date'] = reindexed_df.loc[missing_mask].index + pd.Timedelta(hours=1)
        reindexed_df.reset_index(names=['start_date'], inplace=True)

        ts_dict[prod_type] = reindexed_df

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for prod_type in ts_dict.keys():
        for _, row in ts_dict[prod_type].iterrows():
            cursor.execute(
                ('INSERT INTO production_per_type ' 
                '(prod_start, prod_end, source, country, tenor, production_type, quantity, unit, processing) ' 
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) '
                'ON CONFLICT (prod_start, prod_end, source, country, production_type, processing) DO NOTHING'),

                (row.start_date, 
                 row.end_date, 
                 'RTE', 
                 country, 
                 'PT1H', 
                 prod_type,
                 round(float(row.value), 2), 
                 'MW',
                 row.flag)
            )
    
    conn.commit()
    cursor.close()
    conn.close()
