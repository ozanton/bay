import pg8000
from config.weatherapi import host, port, database, user, password
from utils.weather_logging import logger

class DatabaseConnector:

    def __init__(self):
        # используем уже настроенные правила логирования
        self.logger = logger

        # сохраняем параметры подключения к базе данных
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        # логирование инициализации соединения
        self.logger.info("Инициализация соединения с базой данных")

        try:
            # пытаемся подключиться к базе данных
            connection = pg8000.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        except pg8000.Error as e:
            # если возникла ошибка, то логируем ее
            self.logger.error("Ошибка подключения к базе данных")
            raise

        # логирование успешного подключения
        self.logger.info("Соединение с базой данных установлено")

        return connection


if __name__ == "__main__":
    connector = DatabaseConnector()
    connection = connector.connect()