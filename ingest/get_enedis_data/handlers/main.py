import requests
import json
import os
import boto3
from botocore.client import Config
import datetime as dt
import logging
from urllib.parse import urlencode
import pytz


ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
REGION = "fr-par"


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_enedis_temp(date_obj : dt.date):
    try:
        paris_tz = pytz.timezone('Europe/Paris')
        start = dt.datetime.combine(date_obj, dt.datetime.min.time())
        localized_start = paris_tz.localize(start).astimezone(pytz.utc)
        end = dt.datetime.combine(date_obj + dt.timedelta(days=1), dt.datetime.min.time())
        localized_end = paris_tz.localize(end).astimezone(pytz.utc)

        request_dict = {
            "select": "horodate,temperature_realisee_lissee_degc,temperature_normale_lissee_degc",
            "where": f"horodate >= date'{localized_start.isoformat()}' and horodate < date'{localized_end.isoformat()}'",
            "order_by": "horodate",
            "limit": 100
        }
        endpoint = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/records"
        url = f'{endpoint}?{urlencode(request_dict)}'

        logging.info(f"Requesting data from ENEDIS API with URL: {url}.")
        resp_get = requests.get(url=url, timeout=10)
        if resp_get.status_code == 200:
            logging.info("Data successfully collected from ENEDIS API.")
        else:
            raise Exception("Failed to collect data from ENEDIS API.")
        
        client = boto3.client(
            's3',
            endpoint_url=f'https://s3.{REGION}.scw.cloud',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name=REGION
        )
        key = f'ENEDIS/Temperature/FR/{date_obj.isoformat().replace('-', '')}_ENEDIS_Temp_FR.json'
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
        if not all([ACCESS_KEY, SECRET_KEY]):
            logging.error("Missing required environment variables. Ensure ACCESS_KEY and SECRET_KEY are set.")
            raise EnvironmentError("Environment variable validation failed.")
        
        body = event.get('body', '{}')
        body_data = json.loads(body)
      
        date_str = body_data.get('date', (dt.date.today() - dt.timedelta(days=1)).isoformat())
        date_obj = dt.date.fromisoformat(date_str)

        get_enedis_temp(date_obj)
        return {
            'statusCode': 200,
            'body': f"Operation completed successfully for date {date_str}."
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Operation failed: {str(e)}"
        }
