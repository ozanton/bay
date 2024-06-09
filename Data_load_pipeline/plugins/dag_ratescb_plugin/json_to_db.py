from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR, Date, Boolean, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class TableCbrf(Base):
    __tablename__ = 'rates_cbrf'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    date = Column(TIMESTAMP, nullable=False)
    name = Column(VARCHAR(100), nullable=False)
    code = Column(VARCHAR(10), nullable=False)
    num = Column(Integer, nullable=False)
    rate = Column(Float, nullable=False)

class LoadJsonToCbrfOperator(BaseOperator):
    def __init__(self,
                 postgre_conn: Connection,
                 json_data: str,
                 table_name: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.postgre_conn = postgre_conn
        self.json_data = json_data
        self.table_name = table_name
        self.SQLALCHEMY_DATABASE_URI = f"postgresql://{postgre_conn.login}:{postgre_conn.password}@{postgre_conn.host}:{str(postgre_conn.port)}/{postgre_conn.schema}"

    def execute(self, context):
        engine = create_engine(self.SQLALCHEMY_DATABASE_URI)
        TableClass = getattr(Base, self.table_name)  # Получить класс таблицы по имени
        Base.metadata.create_all(bind=engine)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session_local = SessionLocal()

        # JSON-данные в словарь
        json_data = json.loads(self.json_data)

        # имена столбцов таблицы
        table_columns = [column.name for column in TableClass.__table__.columns]

        # запись в JSON
        for record in json_data:
            # объект таблицы
            new_record = TableClass()

            # атрибуты объекта по сопоставлению
            for json_field, table_field in self.get_mapping(record, table_columns).items():
                setattr(new_record, table_field, record[json_field])

            session_local.add(new_record)
            session_local.commit()

    @staticmethod
    def get_mapping(record: dict, table_columns: list) -> dict:
        mapping = {}
        for json_field, value in record.items():
            if json_field in table_columns:
                mapping[json_field] = json_field
        return mapping