import sys

sys.path.append("..")
from config.windyapi import user, password, host, port, database

SQLALCHEMY_DATABASE_URI = f"postgresql://{user}:{password}@{host}:{port}/{database}"

url = "https://api.windy.com/api/point-forecast/v2"