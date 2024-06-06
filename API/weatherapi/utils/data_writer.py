import csv
from config.config import current_table_name, CURRENT_TABLE_COLUMNS
from utils.database_connector import DatabaseConnector

class DataWriter:

    def __init__(self, filename):
        self.filename = filename

    def write_data(self, data):
        with open(self.filename, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(data.columns)
            writer.writerows(data.to_numpy().tolist())

class DataWriterToDb(DataWriter):

    def __init__(self, filename, conn):
        super().__init__(filename)
        self.conn = conn


    def write_data(self, data):
        super().write_data(data)

        # df в список кортежей
        data_as_tuples = [tuple(row) for row in data.values]

        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.executemany(
                    """
                    INSERT INTO {} ({})
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id_city, time) DO UPDATE
                    SET temp_c = EXCLUDED.temp_c,
                        humidity = EXCLUDED.humidity,
                        wind_kph = EXCLUDED.wind_kph,
                        wind_direction = EXCLUDED.wind_direction,
                        cloud = EXCLUDED.cloud
                    """.format(current_table_name, ','.join(CURRENT_TABLE_COLUMNS)),
                    data_as_tuples,
                )
                try:
                    if self.conn:
                        self.conn.commit()
                except Exception as e:
                    print(f"Ошибка при подтверждении изменений в БД: {e}")
        except Exception as e:
            print(f"Ошибка при записи данных в БД: {e}")

class CitiesWithIds:

    def __init__(self, conn):
        self.conn = conn

    def get_cities_with_ids(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id_city, name_city FROM dict_cities")  # извлечение ID и имени
        cities = dict(cur.fetchall())

        # города в словарь, избегая дубликатов имен
        for city_name in cities:  # Итерация по ключам словаря (имена городов)
            if city_name not in cities:  # Проверка, существует ли ключ уже
                cities[city_name] = cities[city_name] + 1  # Если нет, добавляем город с ID

        return cities


