import psycopg2
import pandas as pd
import os
import numpy as np
import boto3
from botocore.client import Config
import datetime as dt
import logging
import json 
from dateutil.easter import easter
from io import BytesIO


ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
BDD_USER = os.getenv('BDD_USER')
BDD_PASSWORD = os.getenv('BDD_PASSWORD')
REGION = "fr-par"


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_eex_holidays(year : int) -> set:
    holidays = [
        dt.date(year, 1, 1), # New Year
        easter(year) - dt.timedelta(days=2), # Good Friday
        easter(year) + dt.timedelta(days=1), # Easter Monday
        dt.date(year, 5, 1), # Labour Day
        dt.date(year, 12, 24), # Christmas Eve
        dt.date(year, 12, 25), # Christmas Day
        dt.date(year, 12, 26), # Boxing Day
        dt.date(year, 12, 31) # New Year's Eve
    ]
    return set(holidays)


def fetch_file_from_scaleway(key):
    logging.info("Connection to Scaleway Object Storage.")
    client = boto3.client(
        's3',
        endpoint_url=f'https://s3.{REGION}.scw.cloud',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=REGION
    )
    resp_get = client.get_object(
        Bucket='vercloud',
        Key=key
    )
    if resp_get['ResponseMetadata']['HTTPStatusCode'] == 200:
        logging.info(f"File successfully read from Scaleway: {key}")
    else:
        raise Exception(f"Failed to read file from Scaleway.")
    return BytesIO(resp_get['Body'].read())


def process_eex_content(content, shortcodes_df):
    logging.info("Start EEX data processing.")
    raw_lines_df = pd.read_csv(content, comment='#', sep='.', names=['raw_lines'])
    if raw_lines_df.empty:
        raise Exception("The file is empty.")
    
    trading_date_line = raw_lines_df[raw_lines_df.raw_lines.str.startswith('ST')]
    if len(trading_date_line) != 1:
        raise Exception(f'Trading date problem.')
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
    logging.info("End EEX data processing.")
    
    return product_df_clean, trading_date


def insert_data_into_db(product_df, trading_date, country):
    logging.info("Connection to Scaleway PostgreSQL DB.")
    conn = psycopg2.connect(
        host="195.154.197.20",
        port=2248,
        database="verdb",
        user=BDD_USER,
        password=BDD_PASSWORD
    )
    cursor = conn.cursor()

    for _, row in product_df.iterrows():
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
    logging.info("Successfully import data into DB.")


def handler(event, context):
    try:
        if not all([ACCESS_KEY, SECRET_KEY, BDD_PASSWORD, BDD_USER]):
            logging.error("Missing required environment variables. Ensure ACCESS_KEY, SECRET_KEY, BDD_USER and BDD_PASSWORD are set.")
            raise EnvironmentError("Environment variable validation failed.")  

        logging.info("JSON arguments retrieval.")
        body = event.get('body', '{}')
        body_data = json.loads(body)

        date_str = body_data.get('date', (dt.date.today() + dt.timedelta(days=1)).isoformat())
        country = body_data.get('country', 'FR')

        logging.info("Start of EEX data import from Object Storage to PostgreSQL.")

        date_obj = dt.date.fromisoformat(date_str)
        holidays = get_eex_holidays(date_obj.year)

        if (date_obj.weekday() > 4) or (date_obj in holidays):
            return {
                'statusCode': 200,
                'body': f"Operation completed successfully for date {date_str} (not a business day so no data processed)."
            }
        else:
            logging.info(f"Start retrieval of the shortcodes file for {country}.")
            key = f'EEX/Power/Futures/{country}/Products_Shortcodes_Power_Futures_EEX_{country}.xlsx'
            shortcodes_df = pd.read_excel(fetch_file_from_scaleway(key))
            logging.info(f"End retrieval of the shortcodes file for {country}.")
            
            key = f'EEX/Power/Futures/{country}/{date_str.replace('-', '')}_Power_Futures_EEX_{country}.csv'
            content = fetch_file_from_scaleway(key)
            product_df, trading_date = process_eex_content(content, shortcodes_df)
            insert_data_into_db(product_df, trading_date, country)

            return {
                'statusCode': 200,
                'body': f"Operation completed successfully for date {date_str} and country {country}."
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Operation failed: {str(e)}"
        }