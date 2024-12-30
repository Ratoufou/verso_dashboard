import psycopg2
import pandas as pd
import os

def KPLER_Futures_Elec_EEX_process(date, country, dir, DB_CONFIG):
        
    for type in ['Base', 'Peak']:

        file = f'{date}_Futures_Elec_EEX_{country}_{type}.csv'
        path = os.path.join(dir, file)

        if not(os.path.exists(path)):
            continue

        try:    
            df = pd.read_csv(path, index_col=0)
        except pd.errors.EmptyDataError:
            continue

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for _, row in df.iterrows():

            cursor.execute(
                ('INSERT INTO elec_futures_market' 
                 '(trading_date, delivery_start, delivery_end, source, country, peak, tenor, settlement_price, currency)' 
                 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                 'ON CONFLICT (trading_date, delivery_start, delivery_end, source, country, peak) DO NOTHING'),

                (row['trading_date'], 
                 row['delivery_start'], 
                 row['delivery_end'], 
                 'EEX', 
                 country, 
                 type == 'Peak', 
                 row['tenor'], 
                 round(row['settlement_price'], 2), 
                 'EUR')
            )

        conn.commit()
        cursor.close()
        conn.close()

