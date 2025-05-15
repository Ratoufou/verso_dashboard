import boto3
from botocore.client import Config
import os
import logging
import paramiko
import json
from datetime import date, timedelta
from dateutil.easter import easter
from io import BytesIO


EEX_USER = os.getenv('EEX_USER')
EEX_PASSWORD = os.getenv('EEX_PASSWORD')
SFTP_CONFIG = {
    'hostname' : 'datasource.eex-group.com',
    'port' : 22,
    'username' : EEX_USER,
    'password' : EEX_PASSWORD
}
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
REGION = "fr-par"


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_eex_holidays(year : int) -> set:
    holidays = [
        date(year, 1, 1), # New Year
        easter(year) - timedelta(days=2), # Good Friday
        easter(year) + timedelta(days=1), # Easter Monday
        date(year, 5, 1), # Labour Day
        date(year, 12, 24), # Christmas Eve
        date(year, 12, 25), # Christmas Day
        date(year, 12, 26), # Boxing Day
        date(year, 12, 31) # New Year's Eve
    ]
    return set(holidays)


def eex_connexion(sftp_config=SFTP_CONFIG):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(**sftp_config)
    sftp = ssh.open_sftp()
    return ssh, sftp


def get_eex_futures(date_str : str, country : str):
    futures_path = f'/F:/EEX_Production/master/market_data/power/{country.lower()}/derivatives/csv/'

    try:
        ssh, sftp = eex_connexion()
        binary_content = BytesIO()
        file_sftp = os.path.join(futures_path, date_str[:4], date_str, f'PowerFutureResults_{country}_{date_str}.CSV')
        sftp.getfo(remotepath=file_sftp, fl=binary_content)
        sftp.close()
        ssh.close()
        binary_content.seek(0)
        logging.info(f"Length of binary content: {len(binary_content.getvalue())}")

        client = boto3.client(
            's3',
            endpoint_url=f'https://s3.{REGION}.scw.cloud',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name=REGION
        )
        key = f'EEX/Power/Futures/{country}/{date_str}_Power_Futures_EEX_{country}.csv'
        resp_put = client.put_object(
            Bucket='vercloud',
            Key=key,
            Body=binary_content
        )
        if resp_put['ResponseMetadata']['HTTPStatusCode'] == 200:
            logging.info(f"File successfully uploaded to S3: {key}")
        else:
            raise Exception(f"Failed to upload file to Scaleway.") 

    except Exception as e:
        logging.error(f"Error: {e}.")  


def handler(event, context):
    try:
        if not all([EEX_USER, EEX_PASSWORD, ACCESS_KEY, SECRET_KEY]):
            logging.error("Missing required environment variables. Ensure EEX_USER, EEX_PASSWORD, ACCESS_KEY and SECRET_KEY are set.")
            raise EnvironmentError("Environment variable validation failed.")
        
        body = event.get('body', '{}')
        body_data = json.loads(body)
      
        date_str = body_data.get('date', (date.today() - timedelta(days=1)).isoformat())
        country = body_data.get('country', 'FR')

        date_obj = date.fromisoformat(date_str)
        holidays = get_eex_holidays(date_obj.year)

        if (date_obj.weekday() > 4) or (date_obj in holidays):
            return {
                'statusCode': 200,
                'body': f"Operation completed successfully for date {date_str} (not a business day so no data collected)."
            }
        else:
            get_eex_futures(date_str.replace('-', ''), country)
            return {
                'statusCode': 200,
                'body': f"Operation completed successfully for date {date_str} and country {country}."
            }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Operation failed: {str(e)}"
        }