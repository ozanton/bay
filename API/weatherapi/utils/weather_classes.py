import requests
from config.config import api_url
import datetime

class Weather:
    """Атрибуты:
        temp: Температура в градусах Цельсия
        humidity: Влажность в процентах
        pressure: Давление в мм рт. ст.
        wind_speed: Скорость ветра в км/ч
        wind_direction: Направление ветра
        last_updated: Время обновления данных
        cloud: Облачность в процентах
        feelslike_c: Ощущаемая температура в градусах Цельсия
        vis_km: Видимость в километрах
        uv: Индекс ультрафиолетового излучения
    """

    def __init__(
            self,
            temp,
            humidity,
            pressure,
            wind_speed,
            wind_direction,
            last_updated,
            cloud,
            feelslike_c=0.0,
            vis_km=0.0,
            uv=0.0,
    ):
        self.temp = temp
        self.humidity = humidity
        self.pressure = pressure
        self.wind_speed = wind_speed
        self.wind_direction = wind_direction
        self.last_updated = last_updated
        self.cloud = cloud
        self.feelslike_c = feelslike_c
        self.vis_km = vis_km
        self.uv = uv

    def __str__(self):
        return (
            f"Температура: {self.temp} °C\n"
            f"Влажность: {self.humidity}%\n"
            f"Давление: {self.pressure} мм рт. ст.\n"
            f"Скорость ветра: {self.wind_speed} км/ч\n"
            f"Направление ветра: {self.wind_direction}"
        )

class CurrentWeather(Weather):

    def __init__(
            self,
            temp,
            humidity,
            pressure,
            wind_speed,
            wind_direction,
            last_updated,
            cloud,
            feelslike_c=0.0,
            vis_km=0.0,
            uv=0.0,
    ):
        super().__init__(
            temp,
            humidity,
            pressure,
            wind_speed,
            wind_direction,
            last_updated,
            cloud,
            feelslike_c,
            vis_km,
            uv,
        )

    def __str__(self):
        return (
            f"Температура: {self.temp} °C\n"
            f"Влажность: {self.humidity}%\n"
            f"Давление: {self.pressure} мм рт. ст.\n"
            f"Скорость ветра: {self.wind_speed} км/ч\n"
            f"Направление ветра: {self.wind_direction}\n"
            f"Время обновления данных: {self.last_updated}\n"
            f"Облачность: {self.cloud}%\n"
            f"Ощущаемая температура: {self.feelslike_c} °C\n"
            f"Видимость: {self.vis_km} км\n"
            f"Индекс ультрафиолетового излучения: {self.uv}"
        )

    @classmethod
    def get_weather(cls, api_key, city, feelslike_c=0.0, vis_km=0.0, uv=0.0):
        url = f"{api_url}?key={api_key}&q={city}"
        response = requests.get(url)
        if response.status_code == 200:
            res = response.json()
            return cls(
                res['current']['temp_c'],
                res['current']['humidity'],
                res['current']['pressure_mb'],
                res['current']['wind_kph'],
                res['current']['wind_degree'],
                res['current']['last_updated_epoch'],
                res['current']['cloud'],
                feelslike_c,
                vis_km,
                uv,
            )
        else:
            raise Exception(f"Ошибка при получении данных о погоде для города {city}: {response.status_code}")

class WeatherAPI:
    def __init__(self, api_key):
        self.api_key = api_key

    def get_weather(self, city):
        return CurrentWeather.get_weather(
            self.api_key, city, 0.0, 0.0, 0.0
        )

# Тест
#weather_api = WeatherAPI(api_key)
#weather = weather_api.get_weather("Novi Sad")
#print(weather)

class ForecastWeather:
    """Атрибуты:
        forecast_data: Данные прогноза погоды в формате JSON включают в себя следующие поля:

        * date: Дата
        * day: День недели
        * high: Максимальная температура
        * low: Минимальная температура
        * text: Описание погоды
        * icon: Иконка погоды
        * precip: Осадки
        * humidity: Влажность
        * wind_speed: Скорость ветра
        * wind_gust: Порывы ветра
        * cloud_cover: Облачность
        * uv_index: Индекс ультрафиолетового излучения
        * alerts: Предупреждения о погодных явлениях
    """

    def __init__(self, city, forecast_data):
        self.city = city
        self.forecast_data = forecast_data

    def __str__(self):
        return f"Прогноз погоды для города {self.city}\n\n" + "\n".join(
            f"{date}: {day} | {high}°C / {low}°C | {text} | {icon}"
            for date, day, high, low, text, icon in self.forecast_data
        )

    @classmethod
    def get_forecast_weather(cls, api_key, city):
        url = f"{api_url}?key={api_key}&q={city}&days=3&aqi=yes&alerts=yes"
        response = requests.get(url)
        if response.status_code == 200:
            res = response.json()
            return cls(city, res['forecast']['forecastday'])
        else:
            raise Exception(f"Ошибка при получении данных о погоде для города {city}: {response.status_code}")

    def get_hourly_forecast(self):
        """Возвращает почасовой прогноз в виде списка словарей."""
        hourly_forecast = []
        for forecast_day in self.forecast_data:
            for hour in forecast_day['hour']:
                data = {
                    'city': self.city,
                    'hour': hour['time'],
                    'temp_c': hour['temp_c'],
                    'wind_kph': hour['wind_kph'],
                    'cloud': hour['cloud']
                }
                hourly_forecast.append(data)
        return hourly_forecast

    def get_four_hourly_forecast(self):
        """Возвращает прогноз погоды с интервалом в 4 часа в виде списка словарей."""
        four_hourly_forecast = []
        for forecast_day in self.forecast_data:
            for hour in forecast_day['hour']:
                hour_time = datetime.datetime.strptime(hour['time'], '%Y-%m-%d %H:%M').hour  # в число
                if hour_time % 4 == 0:
                    data = {
                        'city': self.city,
                        'hour': hour['time'],
                        'temp_c': hour['temp_c'],
                        'wind_kph': hour['wind_kph'],
                        'cloud': hour['cloud']
                    }
                    four_hourly_forecast.append(data)
        return four_hourly_forecast

