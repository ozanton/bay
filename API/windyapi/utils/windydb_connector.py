from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys

sys.path.append("..")
from config.config import SQLALCHEMY_DATABASE_URI
from model.dbase import Base

class DbConnector:
    def __init__(self):
        try:
            self.engine = create_engine(SQLALCHEMY_DATABASE_URI)
            Base.metadata.create_all(self.engine)
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")
            raise Exception(f"Ошибка подключения к базе данных: {e}")

    def connect(self):
        try:
            connection = self.engine.connect()
            print("Успешное подключение к базе данных.")
            return connection
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")
            raise Exception(f"Ошибка подключения к базе данных: {e}")

# объекта подключения к бд
con = DbConnector()

# сессия для работы с бд
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=con.engine)
session_local = SessionLocal()