import pandas as pd
import scipy.spatial
import numpy as np

weather_stations = pd.read_csv('../data/processed/weather_stations.csv')
weather = pd.read_csv('../data/processed/weather.csv')
air_quality_stations = pd.read_csv('../data/processed/air-quality_stations.csv', sep=';')
air_quality = pd.read_csv('../data/processed/air-quality.csv')

distance_matrix = scipy.spatial.distance_matrix(air_quality_stations.loc[:, ['LATITUD', 'LONGITUD']], weather_stations.loc[:, ['latitud', 'longitud']], p=2)
# Create a dataframe with the distance matrix, where the columns are the weather stations and the rows are the air quality stations
distance_matrix_df = pd.DataFrame(distance_matrix, index=air_quality_stations.CODIGO, columns=weather_stations.indicativo)


def interpolate_weather(weather, air_quality, distance_matrix_df):
    ''' Iterate each date on air quality data, for each air quality station on that date, get an interpolated value of all the weather data columns, for all weather stations combined using inverse distance weighting (IDW) '''
    weather_stations_idw = pd.DataFrame(columns=weather.columns).drop(columns=['altitude'])
    for date in air_quality.date.unique():
        print(date)
        # Iterate each air quality station that exists on that date
        for station in air_quality.loc[air_quality.date == date, 'station'].unique():
            # Get the weather data for that date
            weather_data = weather.loc[weather.date == date].set_index('station')
            # Get the distances for all the
            station_mask = distance_matrix_df.loc[station].index.isin(weather_data.index)
            distances = distance_matrix_df.loc[station][station_mask]
            # Iterate each weather_data column except the first three (date, station, altitude)
            weather_data_idw = pd.Series(dtype='object')
            weather_data_idw['date'] = date
            weather_data_idw['station'] = station
            for column in weather_data.columns[2:]:
                # Get the weather data for that column
                weather_data_column = weather_data[column]
                # Filter the NaN values from the column
                column_mask = weather_data_column.notna()
                weather_data_column = weather_data_column[column_mask]
                # Filter them too from the distances
                distances_column = distances[column_mask]
                # Compute the IDW weights for the weather data for that column and the distances for that column too
                p = 5 # 5 is the best value for this case
                weights = 1 / (distances_column ** p)
                # Compute the IDW interpolated value for that column
                interpolated_value = weather_data_column.multiply(weights).sum() / weights.sum()
                # Add the interpolated value to the weather_data_idw series
                weather_data_idw[column] = interpolated_value
            
            # Append the IDW to the weather stations dataframe
            weather_stations_idw = weather_stations_idw.append(weather_data_idw, ignore_index=True)
        # print(weather_stations_idw)
    return weather_stations_idw  

interpolate_weather(weather, air_quality, distance_matrix_df).to_csv('../data/processed/weather_interpolated.csv', index=False)