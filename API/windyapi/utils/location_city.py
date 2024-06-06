from geopy.geocoders import Nominatim
import io
import json

def get_city_coordinates(city_name):
  """
  Функция для определения координат города по его имени.

  Args:
    city_name (str): Название города.

  Returns:
    tuple: (latitude, longitude) или None, если город не найден.
  """
  geolocator = Nominatim(user_agent="ozanton")
  location = geolocator.geocode(city_name)
  if location:
    return location.latitude, location.longitude
  else:
    print(f"Координаты города {city_name} не найдены.")
    return None

# Список городов
cities = ["Berlin", "Belgrade", "Novi Sad", "Podgorica", "Herceg-Novi", "Tivat", "Limassol", "Tel Aviv", "Saint Petersburg", "Gorno-Altaysk", "Kazan", "Yuzhno-Sakhalinsk", "Tbilisi", "Erevan"]

# Словарь для хранения координат городов
city_coordinates = {}

for city in cities:
  latitude, longitude = get_city_coordinates(city)
  if latitude is not None and longitude is not None:
    city_coordinates[city] = (latitude, longitude)

# Создание виртуального файла
buffer = io.StringIO()
json.dump(city_coordinates, buffer, indent=4)

# Чтение виртуального файла
buffer.seek(0)
city_coordinates_from_buffer = json.load(buffer)

# Вывод координат
for city, coordinates in city_coordinates_from_buffer.items():
  print(f"{city}: {coordinates}")
