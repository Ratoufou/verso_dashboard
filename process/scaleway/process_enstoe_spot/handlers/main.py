import boto3
from botocore.client import Config
import datetime as dt
import logging
import json 
import os
import lxml.etree as et
import pandas as pd 
import psycopg2
from isodate import parse_duration, duration_isoformat


ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
BDD_USER = os.getenv('BDD_USER')
BDD_PASSWORD = os.getenv('BDD_PASSWORD')
REGION = "fr-par"
EIC_CODES = {
    '10YFR-RTE------C' : 'FR'
}


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def fetch_file_from_scaleway(date_str, bidding_zone):
    key = f'ENTSOE/DayAhead/{bidding_zone}/{date_str.replace('-', '')}_DA_price_{bidding_zone}.xml'
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
    return resp_get['Body']


def parse_xml(content):
    logging.info("Start parsing XML content.")
    tree = et.parse(content)
    root = tree.getroot()
    ns = root.nsmap
    ts_elem = root.find('TimeSeries', namespaces=ns)
    country = EIC_CODES[ts_elem.find('in_Domain.mRID', namespaces=ns).text]
    unit = f"{ts_elem.find('currency_Unit.name', namespaces=ns).text}/{ts_elem.find('price_Measure_Unit.name', namespaces=ns).text}"
    period_elem = ts_elem.find('Period', namespaces=ns)
    time_inteval_elem = period_elem.find('timeInterval', namespaces=ns)
    start = pd.Timestamp(time_inteval_elem.find('start', namespaces=ns).text)
    end = pd.Timestamp(time_inteval_elem.find('end', namespaces=ns).text)
    resolution = parse_duration(period_elem.find('resolution', namespaces=ns).text)
    date_range = pd.date_range(start=start, end=end, freq=resolution, inclusive='left').tz_localize(None)
    val = [pd.NA for _ in range(len(date_range))]
    for pt in period_elem.findall('Point', namespaces=ns):
        pos = int(pt.find('position', namespaces=ns).text)
        price = float(pt.find('price.amount', namespaces=ns).text)
        val[pos-1] = price
    df = pd.DataFrame(data=val, index=date_range, columns=['price']).ffill()
    logging.info("End parsing XML content.")
    return df, country, resolution, unit


def insert_data_into_db(df, country, resolution, unit):
    logging.info("Connection to Scaleway PostgreSQL DB.")
    conn = psycopg2.connect(
        host="195.154.197.20",
        port=2248,
        database="verdb",
        user=BDD_USER,
        password=BDD_PASSWORD
    )
    cursor = conn.cursor()
    for timestamp, row in df.iterrows():
                cursor.execute(
            ('INSERT INTO elec_day_ahead_market ' 
             '(delivery_start, delivery_end, source, country, tenor, price, unit) ' 
             'VALUES (%s, %s, %s, %s, %s, %s, %s) '
             'ON CONFLICT (delivery_start, delivery_end, source, country) DO NOTHING'),

            (timestamp, 
             timestamp+resolution, 
             'ENTSOE', 
             country, 
             duration_isoformat(resolution), 
             round(float(row['price']), 2), 
             unit)
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
        bidding_zone = body_data.get('bidding_zone', 'FR')     

        logging.info("Start of ENTSO-E data import from Object Storage to PostgreSQL.")
        xml_content = fetch_file_from_scaleway(date_str, bidding_zone)
        df, country, resolution, unit =  parse_xml(xml_content)
        insert_data_into_db(df, country, resolution, unit)
        return {
            'statusCode': 200,
            'body': f"Operation completed successfully for date {date_str} and zone {bidding_zone}."
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Operation failed: {str(e)}"
        }