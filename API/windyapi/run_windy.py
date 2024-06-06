import requests
import json
import sys
from sqlalchemy.orm import sessionmaker

sys.path.append("..")
from utils.location_city import buffer
from config.windyapi import api_key
from utils.data_json_parser import JsonParser
from utils.data_save import DataSaver
from utils.windydb_connector import DbConnector
from model.dbtable import DbtableLocation, DbtableWindyForecast

# координаты из виртуального Json-файла
buffer.seek(0)
try:
    city_coordinates_from_buffer = json.load(buffer)
except json.JSONDecodeError:
    print("**Ошибка: Неверный формат JSON-файла.**")
    sys.exit(1)

model = "gfs"  # Выбор модели 'EC' or 'GFC'

# параметры данных
parameters = ["temp", "dewpoint", "precip", "snowPrecip", "convPrecip", "wind", "windGust", "ptype", "lclouds", "mclouds", "hclouds",
"rh", "pressure"]

levels = ["surface"]  # Default: ["surface"]

# объект подключения к бд
con = DbConnector()

# сессия для работы с бд
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=con.engine)
session_local = SessionLocal()

def make_windy_api_request(latitude, longitude, model, parameters, levels, api_key):
    """
    Функция отправляет POST-запрос к Windy.com API и возвращает JSON-ответ.
    """
    data = {
        "lat": latitude,
        "lon": longitude,
        "model": model,
        "parameters": parameters,
        "levels": levels,
        "key": api_key,
    }

    url = "https://api.windy.com/api/point-forecast/v2"

    response = requests.post(url, json=data)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Ошибка: {response.status_code}")
        return None

def process_city(city, coordinates, session):
    latitude, longitude = coordinates

    print(f"Запрос для города {city}:")
    print(f"Широта: {latitude}")
    print(f"Долгота: {longitude}")

    json_data = make_windy_api_request(latitude, longitude, model, parameters, levels, api_key)

    if json_data is not None:
        try:
            parser = JsonParser()
            df = parser.parse_windy_json(json_data)

            if df is not None:
                df.rename(columns={
                    'temp_surface': 'temp',
                    'dewpoint_surface': 'dewpoint',
                    'past3hprecip_surface': 'past3hprecip',
                    'past3hsnowprecip_surface': 'past3hsnowprecip',
                    'past3hconvprecip_surface': 'past3hconvprecip',
                    'wind_u_surface': 'wind_u',
                    'wind_v_surface': 'wind_v',
                    'gust_surface': 'gust',
                    'ptype_surface': 'ptype',
                    'lclouds_surface': 'lclouds',
                    'mclouds_surface': 'mclouds',
                    'hclouds_surface': 'hclouds',
                    'rh_surface': 'rh',
                    'pressure_surface': 'pressure'
                }, inplace=True)
                df.insert(0, "latitude", latitude)
                df.insert(1, "longitude", longitude)

                # id_city по координатам
                location_obj = session.query(DbtableLocation).filter_by(latitude=latitude, longitude=longitude).first()
                if location_obj:
                    df['id_city'] = location_obj.id_city
                else:
                    print("Запись с такими координатами не найдена в таблице location.")
                    return

                print(df.to_string())

                data_saver = DataSaver()
                data_saver.save_dataframe_to_csv(df, latitude, longitude)

                # обновление
                for index, row in df.iterrows():
                    existing_entry = session.query(DbtableWindyForecast).filter_by(
                        latitude=row['latitude'], longitude=row['longitude'], timestamp=row['timestamp']
                    ).first()
                    if existing_entry:
                        session.merge(DbtableWindyForecast(**row))
                    else:
                        new_entry = DbtableWindyForecast(**row)
                        session.add(new_entry)

                session.commit()
                print("Данные успешно добавлены и обновлены в таблице windy_forecast.")
            else:
                print("**Ошибка: JSON-ответ не содержит данных.**")

        except Exception as e:
            print(f"Ошибка при сохранении данных: {e}")
            session.rollback()

    else:
        print("**Ошибка: Не удалось получить ответ API.**")

# Обработка каждого города из файла с координатами
for city, coordinates in city_coordinates_from_buffer.items():
    process_city(city, coordinates, session_local)

# Закрытие сессии
session_local.close()