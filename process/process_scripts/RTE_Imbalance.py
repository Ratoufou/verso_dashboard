import os
import psycopg2
import pandas as pd
import json
from isodate import duration_isoformat


def RTE_Imbalance_process(date, country, dir, DB_CONFIG):

    file = f'{date}_RTE_Imbalance_{country}.json'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        print(path)
        return 
    
    with open(path, 'r') as f:
        imbalance_dict = json.load(f)

    if len(imbalance_dict['imbalance_data']) != 1:
        raise Exception(f'Not only one item in imbalance_data for {date}')
    
    df = pd.DataFrame(imbalance_dict['imbalance_data'][0]['values'])
    if df.empty or df.isna().all().all():
        return 

    df.start_date = pd.to_datetime(df.start_date, utc=True).dt.tz_localize(None)
    df.end_date = pd.to_datetime(df.end_date, utc=True).dt.tz_localize(None)
    df['tenor'] = (df.end_date - df.start_date).apply(lambda x: duration_isoformat(x))

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in df.iterrows():

        imbalance = round(float(row['imbalance']), 2) if pd.notna(row['imbalance']) else None
        system_trend = row['system_trend'] if pd.notna(row['system_trend']) else None
        positive_imbalance_settlement_price = round(float(row['positive_imbalance_settlement_price']), 2) if pd.notna(row['positive_imbalance_settlement_price']) else None
        negative_imbalance_settlement_price = round(float(row['negative_imbalance_settlement_price']), 2) if pd.notna(row['negative_imbalance_settlement_price']) else None

        cursor.execute(
            """INSERT INTO imbalance
            (imb_start, imb_end, source, country, tenor, system_trend, imbalance, unit, positive_imbalance_settlement_price, negative_imbalance_settlement_price, currency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (imb_start, imb_end, source, country) DO NOTHING""",
            (row.start_date,
             row.end_date,
             'RTE',
             country,
             row.tenor,
             system_trend,
             imbalance,
             'MWh',
             positive_imbalance_settlement_price,
             negative_imbalance_settlement_price,
             'EUR')
        )

    conn.commit()
    cursor.close()
    conn.close()
