# -*- coding: utf-8 -*-

import logging
import urllib.request
import pandas as pd
from calendar import monthrange
import datetime

def download_data():
    """ Downloads raw data from Madrid's City Council Open Data website into 'data/raw'.
    """
    logger = logging.getLogger(__name__)
    logger.info('Downloading data-set from Madrid Open Data website...')

    # 2018 air quality file
    url = 'https://datos.madrid.es/egob/catalogo/201410-7775096-calidad-aire-diario.csv'
    urllib.request.urlretrieve(url, 'data/raw/2018-air-quality.csv')

    logger.info(f'Succesfully downloaded from {url}')

def number_of_days_in_month(year=2019, month=2):
    return monthrange(year, month)[1]

def process_data():
    """ Process raw data into 'data/processed'
    """
    logger = logging.getLogger(__name__)
    logger.info('Reading data-sets...')

    df = pd.read_csv('data/raw/2018-air-quality.csv', sep=';')

    #df['TECNICA_MUESTREO'] = df['PUNTO_MUESTREO'].apply(lambda row: row.split('_')[-1])

    df_final = pd.DataFrame(columns=['date', 'station', 'CO', 'NO_2', 'NOx', 'O_3', 'PM10', 'PM25', 'SO_2', 'NO',	'BEN', 'EBE', 'MXY',
     'NMHC', 'OXY', 'PXY', 'TCH', 'TOL', 'CH4'])

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
    
    days = []
    for column in df.columns:
        if column.startswith('D'):
            days.append(column)
    
    grouped_muestreo = df.groupby(['MAGNITUD', 'ESTACION'])

    for group_name, df_group in grouped_muestreo:
        chemical_compound = chemical_compounds[group_name[0]]
        station = group_name[1]
        for row_index, row in df_group.iterrows():
            year = row['ANO']
            month = row['MES']
            logger.info(f'Creating data for chemical_compound={chemical_compound} station={station} year={year} month={month} ...')

            for day in range(number_of_days_in_month(row['ANO'], row['MES'])):
                day_column_name = f"{(day + 1):02d}"
                date_string = f'{year}-{month}-{day_column_name}'
                date_time_obj = datetime.datetime.strptime(date_string, '%Y-%m-%d')

                if row['V' + day_column_name] == 'V':
                    new_row = {
                        'date': date_time_obj.date(),
                        'station': station,
                        chemical_compound: row['D' + day_column_name]
                    }
                    
                    df_final = df_final.append(new_row, ignore_index=True)
                    

    df_final.set_index('date', inplace=True)

    df_final.to_csv('data/interim/2018-air-quality.csv')
    
if __name__ == '__main__':
    log_fmt = '%(asctime)s : %(levelname)s : %(filename)s : %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    download_data()
    process_data()
