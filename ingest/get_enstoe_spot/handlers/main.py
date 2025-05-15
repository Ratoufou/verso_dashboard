import requests
from urllib.parse import urlencode
import boto3
from botocore.client import Config
import datetime as dt
import pytz
import logging
import os
import json


TOKEN_ENTSOE = os.getenv('TOKEN_ENTSOE')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
ENDPOINT_ENTSOE = "https://web-api.tp.entsoe.eu/api"
country_to_eic = {
    'FR' : '10YFR-RTE------C'
}
REGION = "fr-par"


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def spot_request(start : dt.datetime, end : dt.datetime, bidding_zone : str) -> str:
    eic_code = country_to_eic[bidding_zone]
    request_dict = {
        'documentType' : 'A44',
        'periodStart' : start.strftime(format='%Y%m%d%H%M'),
        'periodEnd' : end.strftime(format='%Y%m%d%H%M'),
        'out_Domain' : eic_code,
        'in_Domain' : eic_code,
        'securityToken' : TOKEN_ENTSOE
    }
    return f'{ENDPOINT_ENTSOE}?{urlencode(request_dict)}'


def get_spot_xml(date_obj: dt.date, bidding_zone : str):
    try:
        paris_tz = pytz.timezone('Europe/Paris')
        start = dt.datetime.combine(date_obj, dt.datetime.min.time())
        localized_start = paris_tz.localize(start).astimezone(pytz.utc)
        end = dt.datetime.combine(date_obj + dt.timedelta(days=1), dt.datetime.min.time())
        localized_end = paris_tz.localize(end).astimezone(pytz.utc)
        url = spot_request(start=localized_start, end=localized_end, bidding_zone=bidding_zone)

        logging.info(f"Requesting data from ENTSOE API with URL: {url}.")
        resp_get = requests.get(url=url, timeout=10)
        if resp_get.status_code == 200:
            logging.info("Data successfully collected from ENTSOE API.")
        else:
            raise Exception("Failed to collect data from ENTSOE API.")

        client = boto3.client(
            's3',
            endpoint_url=f'https://s3.{REGION}.scw.cloud',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name=REGION
        )
        key = f'ENTSOE/DayAhead/{bidding_zone}/{date_obj.isoformat().replace('-', '')}_DA_price_{bidding_zone}.xml'
        resp_put = client.put_object(
            Bucket='vercloud',
            Key=key,
            Body=resp_get.content
        )
        if resp_put['ResponseMetadata']['HTTPStatusCode'] == 200:
            logging.info(f"File successfully uploaded to S3: {key}")
        else:
            raise Exception(f"Failed to upload file to Scaleway.") 

    except Exception as e:
        logging.error(f"Error: {e}.")  


def handler(event, context):
    try:
        if not all([TOKEN_ENTSOE, ACCESS_KEY, SECRET_KEY]):
            logging.error("Missing required environment variables. Ensure TOKEN_ENTSOE, ACCESS_KEY, and SECRET_KEY are set.")
            raise EnvironmentError("Environment variable validation failed.")
        
        body = event.get('body', '{}')
        body_data = json.loads(body)
      
        date_str = body_data.get('date', (dt.date.today() + dt.timedelta(days=1)).isoformat())
        bidding_zone = body_data.get('bidding_zone', 'FR')

        date_obj = dt.date.fromisoformat(date_str)

        get_spot_xml(date_obj, bidding_zone)
        return {
            'statusCode': 200,
            'body': f"Operation completed successfully for date {date_str} and zone {bidding_zone}."
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Operation failed: {str(e)}"
        }
    