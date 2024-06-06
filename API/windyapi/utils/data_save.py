import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys

sys.path.append("..")
from model.dbtable import Base, DbtableWindyForecast

class DataSaver:
    def __init__(self, base_dir="data_to_base"):
        self.base_dir = base_dir
    def save_dataframe_to_csv(self, df, latitude, longitude):
        try:
            os.makedirs(self.base_dir, exist_ok=True)

            now = pd.to_datetime("now")

            latitude = float(latitude)
            longitude = float(longitude)

            filename = f"{now.strftime('%Y-%m-%d-%H-%M-%S')}_{latitude:.4f}_{longitude:.4f}.csv"

            if 'latitude' in df.columns and 'longitude' in df.columns:
                df.to_csv(os.path.join(self.base_dir, filename), index=False)
                print(f"Данные успешно сохранены в файл: {os.path.join(self.base_dir, filename)}")
                return True
            else:
                print("Ошибка: столбцы 'latitude' и/или 'longitude' не найдены в DataFrame.")
                return False
        except Exception as e:
            print(f"Ошибка при сохранении данных в CSV: {e}")
            return False

class DataSaveToDb:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    def save_dataframe(self, df):
        try:
            data = df.to_dict(orient='records')
            for row in data:
                data_obj = DbtableWindyForecast(**row)
                self.session.add(data_obj)
            self.session.commit()
            print("Данные успешно сохранены в базу данных.")
        except Exception as e:
            print("Ошибка при сохранении данных в базу данных:", e)
            self.session.rollback()
        finally:
            try:
                self.session.close()
            except Exception as e:
                print(f"Ошибка при закрытии сессии: {e}")