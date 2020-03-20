# -*- coding: utf-8 -*-

import logging
import urllib.request
import json
import os
import datetime
import pandas as pd
import numpy as np


def process_air_quality(filepath: str) -> pd.DataFrame:
    # Read file
    df = pd.read_csv(filepath, sep=';')
    # Transpose day columns into rows
    df = pd.wide_to_long(df, stubnames=['D', 'V'], i=[
                         'PROVINCIA', 'MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 'ANO',	'MES'], j='DIA').reset_index()
    # Filter valid data
    df.drop(df[df['V'] == 'N'].index, inplace=True)
    # Unify date into one column
    df['FECHA'] = pd.to_datetime(
        {
            'year': df['ANO'],
            'month': df['MES'],
            'day': df['DIA']
        }, format='%Y/%m/%d')
    # Transform the station names
    df['ESTACION'] = df['PUNTO_MUESTREO'].apply(lambda x: x.split('_')[0])
    # Drop several columns, we don't need them anymore
    df.drop(columns=['PROVINCIA', 'MUNICIPIO', 'PUNTO_MUESTREO',
                     'ANO', 'MES', 'DIA', 'V'], inplace=True)
    # Transpose MAGNITUD rows into columns, filling gaps with NaN values
    df = pd.pivot_table(df, values='D', index=['ESTACION', 'FECHA'], columns=[
                        'MAGNITUD'], fill_value=np.nan).reset_index()
    # Map chemical compounds with their IDs
    chemical_compounds = {
        1: 'SO_2',
        6: 'CO',
        7: 'NO',
        8: 'NO_2',
        9: 'PM25',
        10: 'PM10',
        12: 'NOx',
        14: 'O_3',
        20: 'TOL',
        30: 'BEN',
        35: 'EBE',
        37: 'MXY',
        38: 'PXY',
        39: 'OXY',
        42: 'TCH',
        43: 'CH4',
        44: 'NMHC'
    }
    # Rename the chemical compounds IDs with their names
    df.rename(columns=chemical_compounds, inplace=True)
    # Translate columns
    df.rename(columns={'FECHA': 'date', 'ESTACION': 'station'}, inplace=True)
    # Unname the dataframe
    df.columns.name = None
    # Sort by date and station
    df.sort_values(by=['date', 'station'], inplace=True)

    return df


def download_data(dataset: dict):
    ''' Downloads raw data from Madrid's City Council Open Data website into 'data/raw'.
    '''
    logger = logging.getLogger(__name__)
    logger.info('Downloading data-set from Madrid Open Data website...')

    for dataset_type, value in dataset.items():
        for year, url in value.items():
            filepath = f'data/raw/{dataset_type}_{year}.csv'
            if os.path.isfile(filepath):
                logger.info(f'File {filepath} already exists')
            else:
                urllib.request.urlretrieve(url, filepath)
                logger.info(f'Succesfully downloaded {filepath} from {url}')


process_methods = {
    'air-quality': process_air_quality
}


def process_data(dataset: dict):
    ''' Process raw data into 'data/processed'
    '''
    logger = logging.getLogger(__name__)
    logger.info('Reading data-sets...')

    interim_path = 'data/interim'

    for subdir, dirs, files in os.walk('data/raw'):
        for file in files:
            filepath = os.path.join(subdir, file)
            if filepath.endswith('.csv'):
                logger.info(f'Processing {filepath} ...')
                dataset_type = file.split('_')[0]
                process_methods[dataset_type](filepath).to_csv(os.path.join(interim_path, file), index=False)


if __name__ == '__main__':
    log_fmt = '%(asctime)s : %(levelname)s : %(filename)s : %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    with open('src/data/dataset.json', 'r') as f:
        dataset = json.load(f)

    download_data(dataset)
    process_data(dataset)
