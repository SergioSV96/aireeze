# -*- coding: utf-8 -*-

import logging
import urllib.request
import json
import os


def download_airquality():
    ''' Downloads raw data from Madrid's City Council Open Data website into 'data/raw'.
    '''
    logger = logging.getLogger(__name__)
    logger.info('Downloading data-set from Madrid Open Data website...')

    with open('src/data/air-quality.json', 'r') as f:
        dataset = json.load(f)

    for data in dataset:
        year = data["year"]
        url = data['url']
        filepath = f'data/raw/air-quality_{year}.csv'
        if os.path.isfile(filepath):
            logger.info(f'File {filepath} already exists')
        else:
            urllib.request.urlretrieve(url, filepath)
            logger.info(f'Succesfully downloaded {filepath} from {url}')


if __name__ == '__main__':
    log_fmt = '%(asctime)s : %(levelname)s : %(filename)s : %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    download_airquality()
