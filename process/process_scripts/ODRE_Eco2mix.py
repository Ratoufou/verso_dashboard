import json
import pandas as pd
import os
import psycopg2
from isodate import duration_isoformat
import datetime as dt


def ODRE_Eco2mix_process(date, country, dir, DB_CONFIG):

    # Check if one of the def, cons or tr files exists
    # Load priority is def, cons, tr
    for type in ['def', 'cons', 'tr']:
        file = f'{date}_ODRE_Eco2mix-{type}_{country}.json'
        path = os.path.join(dir, file)
        if os.path.exists(path):
            break
    if not(os.path.exists(path)):
        return None
    
    with open(path, 'r') as f:
        data = json.load(f)

    # Check the data length
    if (data['total_count'] != 96) or (len(data['results']) != 96):
        raise Exception(f'Error in the length of {file}')

    df = pd.DataFrame(data['results'])    
    df.date_heure = pd.to_datetime(df.date_heure, utc=True).dt.tz_localize(None)

    # Remove duplicates
    df = df[['date_heure', 'consommation', 'eolien', 'solaire']].rename(columns={'date_heure' : 'start'}).groupby('start', sort=True).mean()
    # Fill in values for fields not at quarter-hour granularity (the 30min mean should stay the same)
    df.fillna(df.resample('30min').transform('mean'), inplace=True)
    # Add missing timestamps for the Summer to Winter time change
    # Let the values be NaN, they will be interpolated later (field by field)
    date_obj = dt.datetime.strptime(date, '%Y%m%d')
    start = pd.Timestamp(date_obj, tz='Europe/Paris').tz_convert('UTC').tz_localize(None)
    end = pd.Timestamp(date_obj + dt.timedelta(days=1), tz='Europe/Paris').tz_convert('UTC').tz_localize(None)
    time_range = pd.date_range(start=start, end=end, freq='15min', inclusive='left')
    df = df.reindex(time_range)

    tenor = duration_isoformat(pd.Timedelta(minutes=15))

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for col, prod_type in zip(['eolien', 'solaire'], ['WIND', 'SOLAR']):
        part_df = df[[col]].copy()
        missing_mask = part_df[col].isna()
        part_df.loc[missing_mask, 'flag'] = 'Interpolated'
        part_df.loc[~missing_mask, 'flag'] = 'Unchanged'
        part_df[col] = part_df[col].interpolate(method='time')
        part_df.loc[part_df[col] < 0, col] = 0

        for start, row in part_df.iterrows():
            cursor.execute(
                """INSERT INTO production_per_type
                (prod_start, prod_end, source, country, tenor, production_type, quantity, unit, processing)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (prod_start, prod_end, source, country, production_type, processing) DO NOTHING""",

                (start, 
                start + pd.Timedelta(minutes=15), 
                f'ODRE_Eco2mix_{type}', 
                country, 
                tenor, 
                prod_type, 
                round(float(row[col]), 2), 
                'MW', 
                row.flag)
            )

    part_df = df[['consommation']].copy()
    missing_mask = part_df.consommation.isna()
    part_df.loc[missing_mask, 'flag'] = 'Interpolated'
    part_df.loc[~missing_mask, 'flag'] = 'Unchanged'
    part_df.consommation = part_df.consommation.interpolate(method='time')
    part_df.loc[part_df.consommation < 0, col] = 0
    
    for start, row in part_df.iterrows():
        cursor.execute(
            """INSERT INTO consumption
            (cons_start, cons_end, source, country, tenor, curve_type, quantity, unit, processing)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cons_start, cons_end, source, country, curve_type, processing) DO NOTHING""",

            (start, 
            start + pd.Timedelta(minutes=15), 
            f'ODRE_Eco2mix_{type}', 
            country, 
            tenor, 
            'REALISED',
            round(float(row.consommation), 2), 
            'MW',
            row.flag)
        )
    
    conn.commit()
    cursor.close()
    conn.close()
