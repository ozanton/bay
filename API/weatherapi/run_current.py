import pandas as pd
from utils.options_reader import OptionsReaderDb
from config.weatherapi import api_key
from config.config import api_url
from utils.weather_classes import WeatherAPI
from datetime import datetime
from utils.request_sender import RequestSender
from utils.data_parser import DataParser
from utils.data_writer import DataWriterToDb, CitiesWithIds
from utils.database_connector import DatabaseConnector
from difflib import get_close_matches
import os

draft_res = []

# города из бд
reader_db = OptionsReaderDb()
try:
    cities = reader_db.cities_from_db()
except Exception as e:
    print(f"Ошибка при извлечении списка городов из БД: {e}")
    cities = []

# словарь для сопоставления имен городов с ID
cities_map = CitiesWithIds(DatabaseConnector().connect()).get_cities_with_ids()
print(f"Словарь для сопоставления имен городов с ID: {cities_map}")

weather_api = WeatherAPI(api_key)
request_sender = RequestSender()

for city in cities:
    try:
        # get_close_matches для поиска наилучшего совпадения
        matches = get_close_matches(city, cities_map.values())
        if matches:
            matched_city = matches[0]
            data = request_sender.send_get_request(f"{api_url}?key={api_key}&q={matched_city}")

            if data:
                data_parser = DataParser(data)

                time_readable = datetime.fromtimestamp(int(data_parser.get_last_updated_epoch())).strftime('%Y-%m-%d %H:%M:%S')

                entry = {
                    'id_city': next((k for k, v in cities_map.items() if v == matched_city), None),
                    'time': time_readable,
                    'temp_c': data_parser.get_temp_c(),
                    'humidity': data_parser.get_humidity(),
                    'wind_kph': data_parser.get_wind_kph(),
                    'wind_direction': data_parser.get_wind_degree(),
                    'cloud': data_parser.get_cloud(),
                }

                if matched_city in cities_map.values():
                    entry['city'] = matched_city
                    # промежуточный итог в draft_res
                    draft_res.append(entry)
                    print(f"Промежуточный результат: {entry}")
                else:
                    print(f"Город {matched_city} не найден в словаре cities_map.")
        else:
            print(f"Город {city} не найден в словаре cities_map.")

    except Exception as e:
        print(f"Ошибка для {city}: {e}")

# промежуточныe результаты
for i, entry in enumerate(draft_res):
    print(f"Промежуточные результаты {i + 1}: {entry}")

# Фильтрация финального результата (только те записи, у которых есть 'id_city')
final_res = [entry for entry in draft_res if 'id_city' in entry]

# Вывод финального результата
print(f"Финальный результат: {final_res}")

df = pd.DataFrame(final_res, columns=['id_city', 'time', 'temp_c', 'humidity', 'wind_kph', 'wind_direction', 'cloud'])
print(df)

# коннект к бд
db = DatabaseConnector()
# проверка коннекта
try:
    conn = db.connect()
    print("Подключение к БД успешно установлено")
except Exception as e:
    print(f"Ошибка при подключении к БД: {e}")

# есть ли папка
if not os.path.exists("./resultcurrent_to_base"):
    os.mkdir("./resultcurrent_to_base")

# cоздаем объект DataWriterToDb
dw = DataWriterToDb(
    f"./resultcurrent_to_base/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv", conn
)

# проверки при создании записи в файл
try:
    dw.write_data(df)
    print("Запись в файл успешно создана")
except Exception as e:
    print(f"Ошибка при создании записи в файл: {e}")

finally:
    # закрытие коннекта с БД
    if conn:
        try:
            conn.cursor()
            conn.close()
            print("Соединение с БД закрыто")
        except Exception:
            print("Соединение с БД уже закрыто")