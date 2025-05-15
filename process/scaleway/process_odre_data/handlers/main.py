import json
import boto3
from botocore.client import Config
import datetime as dt
import logging
import os
import pandas as pd 
import psycopg2
from isodate import duration_isoformat
from io import BytesIO


ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
BDD_USER = os.getenv('BDD_USER')
BDD_PASSWORD = os.getenv('BDD_PASSWORD')
REGION = "fr-par"


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
        raise Exception(f"Failed to read file from Scaleway: {key}")
    client.close()
    return BytesIO(resp_get['Body'].read())


def transfer_eco2mix(date_str, data_str):
    key = f'ODRE/Eco2mix/FR/{date_str.replace('-', '')}_ODRE_{data_str}_FR.json'
    data = json.load(fetch_file_from_scaleway(key))

    # Check the data length
    if (data['total_count'] != 96) or (len(data['results']) != 96):
        raise Exception(f'Error in the length of the json file ({key}).')

    df = pd.DataFrame(data['results'])    
    df.date_heure = pd.to_datetime(df.date_heure, utc=True).dt.tz_localize(None)

    # Remove duplicates
    df = df[['date_heure', 'consommation', 'eolien', 'solaire']].rename(columns={'date_heure' : 'start'}).groupby('start', sort=True).mean()
    # Fill in values for fields not at quarter-hour granularity (the 30min mean should stay the same)
    df.fillna(df.resample('30min').transform('mean'), inplace=True)
    # Add missing timestamps for the Summer to Winter time change
    # Let the values be NaN, they will be interpolated later (field by field)
    date_obj = dt.datetime.strptime(date_str, '%Y-%m-%d')
    start = pd.Timestamp(date_obj, tz='Europe/Paris').tz_convert('UTC').tz_localize(None)
    end = pd.Timestamp(date_obj + dt.timedelta(days=1), tz='Europe/Paris').tz_convert('UTC').tz_localize(None)
    time_range = pd.date_range(start=start, end=end, freq='15min', inclusive='left')
    df = df.reindex(time_range)

    tenor = duration_isoformat(pd.Timedelta(minutes=15))

    logging.info("Connection to Scaleway PostgreSQL DB.")
    conn = psycopg2.connect(
        host="195.154.197.20",
        port=2248,
        database="verdb",
        user=BDD_USER,
        password=BDD_PASSWORD
    )
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
                f'ODRE_{data_str.replace('-', '_')}', 
                'FR', 
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
    part_df.loc[part_df.consommation < 0, 'consommation'] = 0

    
    for start, row in part_df.iterrows():
        cursor.execute(
            """INSERT INTO consumption
            (cons_start, cons_end, source, country, tenor, curve_type, quantity, unit, processing)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cons_start, cons_end, source, country, curve_type, processing) DO NOTHING""",

            (start, 
            start + pd.Timedelta(minutes=15), 
            f'ODRE_{data_str.replace('-', '_')}', 
            'FR', 
            tenor, 
            'REALISED',
            round(float(row.consommation), 2), 
            'MW',
            row.flag)
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Deconnection from Scaleway PostgreSQL DB.")


def transfer_capa(year):
    key = f'ODRE/Capacities/FR/{year}_ODRE_Capa_FR.json'
    data = json.load(fetch_file_from_scaleway(key))

    if data['total_count'] != 1:
        raise Exception(f'The total_count is not 1 for {key}.')
    if data['results'][0]['annee'] != str(year):
        raise Exception(f'Problem on the year for {key}')
    
    start = pd.Timestamp(dt.date(year, 1, 1)).tz_localize('Europe/Paris').tz_convert('UTC').tz_localize(None)
    end = pd.Timestamp(dt.date(year+1, 1, 1)).tz_localize('Europe/Paris').tz_convert('UTC').tz_localize(None)
    tenor = duration_isoformat(end - start)

    logging.info("Connection to Scaleway PostgreSQL DB.")
    conn = psycopg2.connect(
        host="195.154.197.20",
        port=2248,
        database="verdb",
        user=BDD_USER,
        password=BDD_PASSWORD
    )
    cursor = conn.cursor()

    conversion_dict = {
        'parc_solaire' : 'SOLAR',
        'parc_eolien' : 'WIND'
    }

    for odre_type in conversion_dict.keys():

        cursor.execute(
            """INSERT INTO installed_capacities
            (capa_start, capa_end, source, country, tenor, capacities_type, quantity, unit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (capa_start, capa_end, source, country, capacities_type) DO NOTHING""",

            (start, 
             end, 
             'ODRE', 
             'FR', 
             tenor, 
             conversion_dict[odre_type], 
             round(float(data['results'][0][odre_type]), 2), 
             'MW')
        )

    conn.commit()
    cursor.close()
    conn.close()


def handler(event, context):
    try:
        if not all([ACCESS_KEY, SECRET_KEY, BDD_PASSWORD, BDD_USER]):
            raise EnvironmentError("Environment variable validation failed. Ensure ACCESS_KEY, SECRET_KEY, BDD_USER and BDD_PASSWORD are set.")  

        logging.info("JSON arguments retrieval.")
        body = event.get('body', '{}')
        body_data = json.loads(body)

        date_str = body_data.get('date', (dt.date.today() - dt.timedelta(days=1)).isoformat())
        data_str = body_data.get('data', 'Eco2mix-tr')    

        date_obj = dt.date.fromisoformat(date_str)

        logging.info(f"Start of ODRE {data_str} data (date {date_str}) import from Object Storage to PostgreSQL.")
        if data_str in ['Eco2mix-tr','Eco2mix-cons', 'Eco2mix-def']:
            transfer_eco2mix(date_str, data_str)
        elif data_str == 'capa':
            transfer_capa(date_obj.year)
        else:
            raise Exception(f'{data_str} is not allowed for the data argument.')

        return {
            'statusCode': 200,
            'body': f"Operation completed successfully for date {date_str} and data {data_str}."
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Operation failed: {str(e)}"
        }