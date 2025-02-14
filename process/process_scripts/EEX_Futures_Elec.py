import psycopg2
import pandas as pd
import os
import numpy as np

pd.set_option('future.no_silent_downcasting', True)

def EEX_Futures_Elec_process(date, country, dir, DB_CONFIG):

    product_shortcodes_file = f'Products_Shortcodes_Power_Futures_EEX_{country}.xlsx'
    try:
        shortcodes_df = pd.read_excel(os.path.join(dir, product_shortcodes_file))
    except:
        raise Exception(f'Problem with the shortcodes file for {country}')

    file = f'{date}_Power_Futures_EEX_{country}.csv'
    path = os.path.join(dir, file)

    if not(os.path.exists(path)):
        return
    
    try:
        raw_lines_df = pd.read_csv(path, comment='#', sep='.', names=['raw_lines'])
    except pd.errors.EmptyDataError:
        return
    
    if raw_lines_df.empty:
        return

    trading_date_line = raw_lines_df[raw_lines_df.raw_lines.str.startswith('ST')]
    if len(trading_date_line) != 1:
        raise Exception(f'Trading date problem for {file}')
    else:
        trading_date = trading_date_line.iloc[0]['raw_lines'].split(';')[1]

    product_df = raw_lines_df[raw_lines_df.raw_lines.str.startswith('PR')]
    product_df = product_df.raw_lines.str.split(';', expand=True)
    product_df.columns = ['Line Type', 'Product', 'Long Name', 'Maturity', 'Delivery Start', 'Delivery End', 'Open Price', 'Timestamp Open Price',
        'High Price', 'Timestamp High Price', 'Low Price', 'Timestamp Low Price', 'Last Price', 'Timestamp Last Price', 
        'Settlement Price', 'Unit of Prices', 'Lot Size', 'Traded Lots', 'Number of Trades', 'Traded Volume', 'Open Interest Lots',
        'Open Interest Volume', 'Unit of Volumes']
    product_df = product_df[['Product', 'Delivery Start', 'Delivery End', 'Settlement Price']]
    product_df = pd.merge(left=product_df, left_on='Product', right=shortcodes_df, right_on='Product')
    
    product_df_clean = pd.concat(
        [product_df[['Delivery Start', 'Delivery End']],
        product_df['Name'].str.split(expand=True)[[3, 4]].rename(columns={3 : 'type', 4 : 'tenor'}),
        product_df['Settlement Price'].str.replace(',', '.').replace('', np.nan).astype(float).infer_objects(copy=False)],
        axis=1
    ).dropna()

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in product_df_clean.iterrows():
            
        cursor.execute(
            """INSERT INTO elec_futures_market
            (trading_date, delivery_start, delivery_end, source, country, peak, tenor, settlement_price, unit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trading_date, delivery_start, delivery_end, source, country, peak) DO NOTHING;""",

            (trading_date, 
             row['Delivery Start'],
             row['Delivery End'],
             'EEX',
             country,
             row['type'] == 'Peak',
             row['tenor'],
             round(row['Settlement Price'], 2),
             'EUR/MWH')
        )

    conn.commit()
    cursor.close()
    conn.close()
