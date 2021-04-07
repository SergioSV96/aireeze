# -*- coding: utf-8 -*-

import logging
import urllib.request
import json
import os
import datetime
import time
import requests
import urllib3
import pandas as pd


def download_airquality():
    ''' Downloads raw data from Madrid's City Council Open Data website into 'data/raw'.
    '''
    logger = logging.getLogger(__name__)
    logger.info('Downloading data-set from Madrid Open Data website...')

    with open('src/data/air-quality.json', 'r') as f:
        dataset = json.load(f)

    for data in dataset:
        year = data['year']
        url = data['url']
        filepath = f'data/raw/air-quality_{year}.csv'
        #urllib.request.urlretrieve(url, filepath)
        r = requests.get(url)
        with open(filepath, 'wb') as outfile:
            outfile.write(r.content)
        logger.info(
            f'Succesfully downloaded air quality data {filepath} from {url}')

        '''
        if os.path.isfile(filepath):
            logger.info(f'File {filepath} already exists')
        else:
            urllib.request.urlretrieve(url, filepath)
            logger.info(f'Succesfully downloaded {filepath} from {url}')
        '''


def download_weather():
    ''' Downloads raw data from AEMET Open Data website into 'data/raw'.
    '''
    logger = logging.getLogger(__name__)

    urllib3.disable_warnings()
    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += 'HIGH:!DH:!aNULL'
    try:
        requests.packages.urllib3.contrib.pyopenssl.DEFAULT_SSL_CIPHER_LIST += 'HIGH:!DH:!aNULL'
    except AttributeError:
        # no pyopenssl support used / needed / available
        pass

    stations_url = f'https://opendata.aemet.es/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones'

    with open('src/data/aemet_api_key.json', 'r') as f:
        api_key = json.load(f)

    headers = {
        'cache-control': 'no-cache'
    }

    logger.info(
        f'Fetching Madrid stations identifiers from {stations_url} ...')

    response = requests.request(
        'GET', stations_url, headers=headers, params=api_key, verify=False).json()
    stations = requests.request('GET', response['datos']).json()
    madrid_stations_id = []

    for station in stations:
        if station['provincia'] == 'MADRID':
            madrid_stations_id.append(station['indicativo'])

    logger.info(f'Madrid stations identifiers: {madrid_stations_id}')

    start_date = datetime.date(2001, 1, 1)
    end_date = datetime.date(2001, 12, 31)
    today = datetime.date.today()

    BACKOFF_FACTOR = 20  # seconds

    while start_date <= today:
        year = start_date.year
        filepath = f'data/raw/weather_{year}.csv'
        dataframe_yearly = pd.DataFrame()
        for station_id in madrid_stations_id:
            url = f'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start_date}T00:00:00UTC/fechafin/{end_date}T23:59:59UTC/estacion/{station_id}/'
            response = requests.request(
                'GET', url, headers=headers, params=api_key, verify=False).json()
            total_retries = 0
            while response['estado'] == 429:
                logger.warning(
                    f'API limit reached, number of total retries is {total_retries+1}, retrying ...')
                if total_retries == 0:
                    response = requests.request(
                        'GET', url, headers=headers, params=api_key, verify=False).json()
                else:
                    time.sleep(BACKOFF_FACTOR * 2**(total_retries - 1))
                    response = requests.request(
                        'GET', url, headers=headers, params=api_key, verify=False).json()

                total_retries += 1

            try:
                data = requests.request('GET', response['datos']).json()
                dataframe_yearly = dataframe_yearly.append(
                    pd.DataFrame(data), ignore_index=True)
                logger.info(
                    f'Succesfully downloaded weather data for {year} from station {station_id}')
            except Exception as e:
                logger.warning(f'No data for {station_id} for the year {year}')

        dataframe_yearly.to_csv(f'data/raw/weather_{year}.csv',
                                mode='w', index=False)
        start_date = datetime.date(start_date.year + 1, 1, 1)
        end_date = datetime.date(end_date.year + 1, 12, 31)
        if end_date > today:
            end_date = today


def clean_workspace():
    directory = 'data/raw/'
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            os.remove(os.path.join(directory, filename))


if __name__ == '__main__':
    log_fmt = '%(asctime)s : %(levelname)s : %(filename)s : %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    clean_workspace()
    download_airquality()
    download_weather()
