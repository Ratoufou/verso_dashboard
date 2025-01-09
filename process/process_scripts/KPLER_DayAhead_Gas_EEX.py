import psycopg2
import pandas as pd
import os

def KPLER_DayAhead_Gas_EEX_process(date, country, dir, DB_CONFIG):

    file = f'{date}_DayAhead_Gas_EEX_{country}.csv'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        return None

    try:
        df = pd.read_csv(path, index_col=0)
    except pd.errors.EmptyDataError:
        return None

    df['tenor'] = df['tenor'].replace({'DA' : 'Day', 'WE' : 'Weekend'})
    df.trading_date = pd.to_datetime(df.trading_date).dt.date
    df.delivery_start = pd.to_datetime(df.delivery_start).dt.date
    df.delivery_end = pd.to_datetime(df.delivery_end).dt.date

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in df.iterrows():

        cursor.execute(
            ('INSERT INTO gas_day_ahead_market ' 
             '(trading_date, delivery_start, delivery_end, source, country, tenor, last_price, currency) ' 
             'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) '
             'ON CONFLICT (trading_date, delivery_start, delivery_end, source, country) DO NOTHING'),

            (row['trading_date'], 
             row['delivery_start'], 
             row['delivery_end'], 
             'EEX', 
             country, 
             row['tenor'], 
             round(row['price'], 2), 
             'EUR')
        )

    conn.commit()
    cursor.close()
    conn.close()
