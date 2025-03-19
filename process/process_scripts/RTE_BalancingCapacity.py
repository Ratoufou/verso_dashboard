import os
import psycopg2
import pandas as pd
import json
from isodate import duration_isoformat
import datetime as dt


def RTE_BalancingCapacity_process(date, country, dir, DB_CONFIG):

    date_obj = dt.date.fromisoformat(f'{date[:4]}-{date[4:6]}-{date[6:]}')
    if date_obj < dt.date(2025, 1, 1):
        DELTA_T = pd.Timedelta(minutes=30)
    else:
        DELTA_T = pd.Timedelta(minutes=15)


    file = f'{date}_RTE_BalancingCapacity_{country}.json'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        return None 
    
    with open(path, 'r') as f:
        data = json.load(f)

    ts_dict = dict()
    for e in data['procured_reserves']:
        reserve_type = e['type']
        start = pd.Timestamp(e['start_date'], tz='UTC').tz_localize(None)   
        end = pd.Timestamp(e['end_date'], tz='UTC').tz_localize(None)  
        time_range = pd.date_range(start=start, end=end, freq=DELTA_T, inclusive='left')

        if (reserve_type == 'FCR') or (reserve_type == 'AFRR'):

            if len(e['values']) == 0:
                reindexed_df = pd.DataFrame({
                    'start_date' : list(time_range),
                    'price' : [pd.NA for _ in range(len(time_range))]
                })

            else:
                df = pd.DataFrame(e['values'])
                df.start_date = pd.to_datetime(df.start_date, utc=True).dt.tz_localize(None)
                df.end_date = pd.to_datetime(df.end_date, utc=True).dt.tz_localize(None)

                if not(date_obj == dt.date(2025, 1, 1)):
                    df['timedelta'] = df.end_date - df.start_date
                    timedeltas = df.timedelta.unique()
                    if (len(timedeltas) != 1) or (timedeltas[0] != DELTA_T):
                        raise Exception(f'An interval does not last {duration_isoformat(DELTA_T)} for {date} - {reserve_type} : {list(timedeltas)}')
                
                df.drop_duplicates(subset=['start_date'], inplace=True)
                reindexed_df = df.set_index('start_date').reindex(time_range).reindex(columns=['price'])
                reindexed_df.reset_index(names=['start_date'], inplace=True)

            ts_dict[reserve_type] = reindexed_df

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for reserve_type in ts_dict.keys():
        for _, row in ts_dict[reserve_type].iterrows():
            price = round(float(row.price), 2) if pd.notna(row.price) else None

            cursor.execute(
                """INSERT INTO elec_reserve_market  
                (reserve_start, reserve_end, source, country, reserve_type, tenor, price, unit)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (reserve_start, reserve_end, source, country, reserve_type) DO NOTHING""",

                (row.start_date, 
                 row.start_date + DELTA_T, 
                 'RTE', 
                 country, 
                 reserve_type, 
                 duration_isoformat(DELTA_T),
                 price, 
                 'EUR/MW')
            )
    
    conn.commit()
    cursor.close()
    conn.close()