from sqlalchemy import Column, Integer, Float, String, TIMESTAMP, MetaData, PrimaryKeyConstraint, exists
import numpy as np
import sys

sys.path.append("..")
from model.dbase import Base

class DbtableLocation(Base):
    __tablename__ = 'location'
    __table_args__ = {'extend_existing': True}

    id_city = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    @classmethod
    def is_location_exists(cls, session, latitude, longitude):
        return session.query(exists().where((cls.latitude == latitude) & (cls.longitude == longitude))).scalar()

class DbtableWindyForecast(Base):
    __tablename__ = 'windy_forecast'
    __table_args__ = {'extend_existing': True}

    id_city = Column(Integer, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    timestamp = Column(TIMESTAMP, nullable=False)
    temp = Column(Float, nullable=False)
    dewpoint = Column(Float, nullable=False)
    past3hprecip = Column(Float, nullable=False)
    past3hsnowprecip = Column(Integer, nullable=False)
    past3hconvprecip = Column(Float, nullable=False)
    wind_u = Column(Float, nullable=False)
    wind_v = Column(Float, nullable=False)
    gust = Column(Float, nullable=False)
    ptype = Column(Integer, nullable=False)
    lclouds = Column(Float, nullable=False)
    mclouds = Column(Float, nullable=False)
    hclouds = Column(Float, nullable=False)
    rh = Column(Float, nullable=False)
    pressure = Column(Float, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('latitude', 'longitude', 'timestamp'),
        {},
    )

    @staticmethod
    def create_table_from_dataframe(session, df):
        # id_city в датафрейм
        latitude = df['latitude'].iloc[0]
        longitude = df['longitude'].iloc[0]
        location_obj = session.query(DbtableLocation).filter_by(latitude=latitude, longitude=longitude).first()
        if location_obj:
            df['id_city'] = location_obj.id_city
        else:
            print("Запись с такими координатами не найдена в таблице location.")
            return

        # наличие полей в таблице
        existing_columns = [column.name for column in DbtableWindyForecast.__table__.columns]

        # поля из датафрейма
        for column_name, column_data in df.items():
            if column_name not in existing_columns:
                column_type = DbtableWindyForecast.get_column_type(column_data)
                if column_type is not None:
                    new_column = Column(column_name, column_type)
                    DbtableWindyForecast.__table__.append_column(new_column)
                    existing_columns.append(column_name)

        # creat to db
        DbtableWindyForecast.__table__.create(session.bind, checkfirst=True)

        # df to table
        session.bulk_insert_mappings(DbtableWindyForecast, df.to_dict(orient='records'))

        print(f"Данные из DataFrame успешно сохранены в таблицу {DbtableWindyForecast.__tablename__}")

    @staticmethod
    def get_column_type(column_data):
        """Возвращает соответствующий тип данных SQLAlchemy для столбца DataFrame."""
        column_dtype = column_data.dtype

        if np.issubdtype(column_dtype, np.floating):
            print(f"Тип данных для столбца: {column_data.name} - Float")
            return Float
        elif np.issubdtype(column_dtype, np.datetime64):
            print(f"Тип данных для столбца: {column_data.name} - TIMESTAMP")
            return TIMESTAMP
        elif np.issubdtype(column_dtype, np.integer):
            print(f"Тип данных для столбца: {column_data.name} - Integer")
            return Integer
        elif np.issubdtype(column_dtype, np.object_):
            print(f"Тип данных для столбца: {column_data.name} - String")
            if column_data.nunique() <= 2:
                return Integer
            elif column_data.nunique() <= 255:
                return String(255)
            else:
                return String(column_data.str.len().max())
        else:
            print(f"Не удалось определить тип данных для столбца: {column_data.name}")
            return None