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


def get_token_and_type(rte_id_64):
    url = "https://digital.iservices.rte-france.com/token/oauth/"
    headers = {"Authorization": f"Basic {rte_id_64}"}
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        resp_dict = response.json()
        return resp_dict['access_token'], resp_dict['token_type']
    else:
        raise Exception(f"Failed to get the RTE API access tocken (status_code {response.status_code}, message {response.text}).")


def get_daily_data_url_key(date_obj, data_str):

    paris_tz = pytz.timezone('Europe/Paris')
    start = dt.datetime.combine(date_obj, dt.datetime.min.time())
    localized_start = paris_tz.localize(start).astimezone(pytz.utc)
    end = dt.datetime.combine(date_obj + dt.timedelta(days=1), dt.datetime.min.time())
    localized_end = paris_tz.localize(end).astimezone(pytz.utc) 

    