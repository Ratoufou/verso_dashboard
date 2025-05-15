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


def get_eco2mix_tr_url(start, end):
    endpoint = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-tr/records"
    request_dict = {
        "where": f"date_heure >= date'{start}' and date_heure < date'{end}'",
        "order_by": "date_heure",
        "limit": 100
    }
    return f'{endpoint}?{urlencode(request_dict)}'


def get_eco2mix_cons_url(start, end):
    endpoint = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-cons-def/records"
    request_dict = {
        "where": f"nature = 'Données consolidées' and date_heure >= date'{start}' and date_heure < date'{end}'",
        "order_by": "date_heure",
        "limit": 100        
    }
    return f'{endpoint}?{urlencode(request_dict)}'


def get_eco2mix_def_url(start, end):
    endpoint = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-cons-def/records"
    request_dict = {
        "where": f"nature = 'Données définitives' and date_heure >= date'{start}' and date_heure < date'{end}'",
        "order_by": "date_heure",
        "limit": 100        
    }
    return f'{endpoint}?{urlencode(request_dict)}'


def get_eco2mix_url_key(data_str, date_obj):
    function_dict = {
        'Eco2mix-tr': get_eco2mix_tr_url,
        'Eco2mix-cons': get_eco2mix_cons_url,
        'Eco2mix-def': get_eco2mix_def_url
    }
    
    paris_tz = pytz.timezone('Europe/Paris')
    start = dt.datetime.combine(date_obj, dt.datetime.min.time())
    localized_start = paris_tz.localize(start).astimezone(pytz.utc)
    end = dt.datetime.combine(date_obj + dt.timedelta(days=1), dt.datetime.min.time())
    localized_end = paris_tz.localize(end).astimezone(pytz.utc)

    url = function_dict[data_str](localized_start.isoformat(), localized_end.isoformat())
    key = f'ODRE/Eco2mix/FR/{date_obj.isoformat().replace('-', '')}_ODRE_{data_str}_FR.json'

    return url, key 


def get_capacities_url_key(year):
    endpoint = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/parc-prod-par-filiere/records"
    request_dict = {
        "where": f"annee = date'{year}'"
    }
    url = f'{endpoint}?{urlencode(request_dict)}'
    key = f'ODRE/Capacities/FR/{year}_ODRE_Capa_FR.json'

    return url, key


def transfer_data(url, key):
    logging.info(f"Requestion data from ODRE API with URL: {url}.")
    resp_get = requests.get(url=url, timeout=10)
    if resp_get.status_code == 200:
        logging.info("Data successfully collected from ODRE API.")
    else:
        raise Exception("Failed to collect data from ODRE API.")
    
    client = boto3.client(
        's3',
        endpoint_url=f'https://s3.{REGION}.scw.cloud',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=REGION
    )
    resp_put = client.put_object(
        Bucket='vercloud',
        Key=key,
        Body=resp_get.content
    )
    if resp_put['ResponseMetadata']['HTTPStatusCode'] == 200:
        logging.info(f"File successfully uploaded to S3: {key}")
    else:
        raise Exception(f"Failed to upload file to Scaleway.") 


def handler(event, context):
    try:
        if not all([ACCESS_KEY, SECRET_KEY]):
            logging.error("Missing required environment variables. Ensure ACCESS_KEY and SECRET_KEY are set.")
            raise EnvironmentError("Environment variable validation failed.")
        
        body = event.get('body', '{}')
        body_data = json.loads(body)
      
        date_str = body_data.get('date', (dt.date.today() - dt.timedelta(days=1)).isoformat())
        data_str = body_data.get('data', 'Eco2mix-tr')

        date_obj = dt.date.fromisoformat(date_str)

        if data_str in ['Eco2mix-tr','Eco2mix-cons', 'Eco2mix-def']:
            url, key = get_eco2mix_url_key(data_str, date_obj)
            transfer_data(url, key)

        elif data_str == 'capa':
            url, key = get_capacities_url_key(date_obj.year)
            transfer_data(url, key)

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