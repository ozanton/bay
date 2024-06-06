
cities_file = 'config/cities.yaml'

api_url = 'https://api.weatherapi.com/v1/current.json'

api_url_f = 'https://api.weatherapi.com/v1/forecast.json'

current_table_name = "weatherapi_current"

forecast_table_name = "weatherapi_forecast"

CURRENT_TABLE_COLUMNS = [
    "id_city",
    "time",
    "temp_c",
    "humidity",
    "wind_kph",
    "wind_direction",
    "cloud",
]

FORECAST_TABLE_COLUMNS = [
    "id_city",
    "hour",
    "temp_c",
    "wind_kph",
    "cloud",
]

LOG_TO_STDOUT = True


#echo $API_KEY


# import os
#
# api_key = os.environ.get('API_KEY')

# host = os.environ.get('HOST')

# port = os.environ.get('PORT')
#
# database = os.environ.get('DATABASE')
#
# user = os.environ.get('USER')
#
# password = os.environ.get('PASSWORD')

# if api_key:
#     print(api_key)
# else:
#     print('Переменная среды API_KEY не установлена')