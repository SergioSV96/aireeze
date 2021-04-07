# -*- coding: utf-8 -*-

import logging
import os
import pandas as pd
import numpy as np
import glob


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


def process_weather(filepath: str) -> pd.DataFrame:
    # Read file
    df = pd.read_csv(filepath)
    # Drop several columns, we don't need them anymore
    df.drop(columns=['nombre', 'provincia', 'horatmin', 'horatmax',
                     'horaPresMax', 'horaPresMin', 'horaracha'], inplace=True)
    # Translate columns
    df.rename(columns={
        'fecha': 'date',
        'indicativo': 'station',
        'altitud': 'altitude',
        'tmed': 'average_temperature',
        'prec': 'rainfall',
        'tmin': 'minimum_temperature',
        'tmax': 'maximum_temperature',
        'dir': 'wind_direction',
        'velmedia': 'average_wind_speed',
        'racha': 'maximum_wind_speed',
        'sol': 'maximum_ultraviolet_index',
        'presMax': 'maximum_pressure',
        'presMin': 'minimum_pressure'
    }, inplace=True)

    # Replace Ip values in rainfall with 0,0 , this is temporary
    df['rainfall'] = df['rainfall'].str.replace('Ip', '0,0')
    # Replace commas with dots, this is for decimal representation
    df = df.replace(',', '.', regex=True)

    # Set column types
    df["date"] = df["date"].apply(pd.to_datetime)
    df["station"] = df["station"].astype("string")
    df["altitude"] = df["altitude"].apply(pd.to_numeric)
    df["average_temperature"] = df["average_temperature"].apply(pd.to_numeric)
    df["rainfall"] = df["rainfall"].apply(pd.to_numeric)
    df["minimum_temperature"] = df["minimum_temperature"].apply(pd.to_numeric)
    df["maximum_temperature"] = df["maximum_temperature"].apply(pd.to_numeric)
    df["average_wind_speed"] = df["average_wind_speed"].apply(pd.to_numeric)
    df["maximum_ultraviolet_index"] = df["maximum_ultraviolet_index"].apply(
        pd.to_numeric)
    df["maximum_pressure"] = df["maximum_pressure"].apply(pd.to_numeric)
    df["minimum_pressure"] = df["minimum_pressure"].apply(pd.to_numeric)
    df["wind_direction"] = df["wind_direction"].apply(pd.to_numeric)
    df["maximum_wind_speed"] = df["maximum_wind_speed"].apply(pd.to_numeric)

    # Sort by date and station
    df.sort_values(by=['date', 'station'], inplace=True)

    return df


process_methods = {
    'air-quality': process_air_quality,
    'weather': process_weather
}


def process_data():
    ''' Process raw data into 'data/interim'
    '''
    logger = logging.getLogger(__name__)
    logger.info('Reading data-sets...')

    interim_path = 'data/interim'

    for subdir, _, files in os.walk('data/raw'):
        for file in files:
            filepath = os.path.join(subdir, file)
            if filepath.endswith('.csv'):
                try:
                    logger.info(f'Processing {filepath} ...')
                    dataset_type = file.split('_')[0]
                    process_methods[dataset_type](filepath).to_csv(
                        os.path.join(interim_path, file), index=False)
                    logger.info(
                        f'Succesfully processed {dataset_type} data for {filepath}')
                except KeyError:
                    logger.error(
                        f"Can't process {filepath}, not method found for this file type")


def join_data():
    ''' Process raw data into 'data/processed'
    '''
    logger = logging.getLogger(__name__)
    logger.info('Reading data-sets...')

    processed_path = 'data/processed'

    for key in process_methods:
        logger.info(f"Processing {key} files")
        df_list = []
        for file in glob.glob(f"data/interim/{key}*.csv"):
            df_list.append(pd.read_csv(file))
        logger.info(f"Joining all {key} data in a unique file")
        df = pd.concat(df_list)
        df.to_csv(os.path.join(processed_path, f"{key}.csv"), index=False)
        logger.info(
            f'Succesfully joined all {key} data in {processed_path}/{key}.csv')


if __name__ == '__main__':
    log_fmt = '%(asctime)s : %(levelname)s : %(filename)s : %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    process_data()
    join_data()
