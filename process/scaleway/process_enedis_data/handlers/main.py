import boto3
from botocore.client import Config
import datetime as dt
import logging
import os
import json
import pandas as pd 
import psycopg2
from io import BytesIO
import numpy as np
from isodate import duration_isoformat


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
        raise Exception(f"Failed to read file from Scaleway.")
    client.close()
    return BytesIO(resp_get['Body'].read())


def transfer_enedis_temp(date_str):
    key = f'ENEDIS/Temperature/FR/{date_str.replace('-', '')}_ENEDIS_Temp_FR.json'
    data = json.load(fetch_file_from_scaleway(date_str))
    if data['total_count'] == 0:
        raise Exception(f'The file {key} is empty.')

    df = pd.DataFrame(data['results']).rename(columns={
        'temperature_normale_lissee_degc' : 'tn',
        'temperature_realisee_lissee_degc' : 'tr'
    }).sort_values(by='horodate')
    df['start'] = pd.to_datetime(df.horodate).dt.tz_convert('UTC').dt.tz_localize(None)
    df['trs'] = np.where(df['tr'] < 15, df['tr'], df['tn'])
    df['delta_t'] = df['tr'] - df['tn']
    df['delta_ts'] = df['trs'] - df['tn']

    conn = psycopg2.connect(
        host="195.154.197.20",
        port=2248,
        database="verdb",
        user=BDD_USER,
        password=BDD_PASSWORD
    )
    cursor = conn.cursor()
    timedelta = df['start'][1] - df['start'][0]
    tenor = duration_isoformat(timedelta)
    df['end'] = df['start'] + timedelta

    for _, row in df.iterrows():
        cursor.execute(
            """INSERT INTO temperature
            (temp_start, temp_end, source, country, tenor, tn, tr, trs, delta_t, delta_ts, unit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (temp_start, temp_end, source, country) DO NOTHING""",

            (row.start,
             row.end,
             'ENEDIS_API',
             'FR',
             tenor,
             row.tn,
             row.tr,
             row.trs,
             row.delta_t,
             row.delta_ts,
             '°C')
        )
        
    conn.commit()
    cursor.close()
    conn.close()


def handler(event, context):
    try:
        if not all([ACCESS_KEY, SECRET_KEY, BDD_USER, BDD_PASSWORD]):
            logging.error("Missing required environment variables. Ensure ACCESS_KEY, SECRET_KEY, BDD_USER and BDD_PASSWORD are set.")
            raise EnvironmentError("Environment variable validation failed.")  
        
        logging.info("JSON arguments retrieval.")
        body = event.get('body', '{}')
        body_data = json.loads(body)

        date_str = body_data.get('date', (dt.date.today() - dt.timedelta(days=1)).isoformat())

        logging.info("Start of ENEDIS data import from Object Storage to PostgreSQL.")
        transfer_enedis_temp(date_str)
        return {
            'statusCode': 200,
            'body': f"Operation completed successfully for date {date_str}."
        }
        
    except Exception as e:
        logging.error(f"Error in handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Operation failed: {str(e)}"
        }