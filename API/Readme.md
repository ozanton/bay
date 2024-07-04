# Receiving data from the API overview
1. Receiving current weather data from the weatherapi.com API for a list of cities from a .yaml file and/or the corresponding database dictionary. The received data is saved in the posrgresql database for further processing with control of data loading through a .csv file.

Consists of the following modules:

config - module contains constants;
data_parser - the module is responsible for processing the received response;
data_writer - module for writing received data to csv or to a database;
database_connector - the module is responsible for connecting to the database;
options_reader - the module is responsible for obtaining a list of cities for a request from a .yaml file or from the corresponding database dictionary;
request_sender - the module is responsible for sending the request;
weather_classes - module contains classes and functions of the received data;
run_current.py(sh) - launcher module.

2. Receiving forecast weather data from the windy.com API by geographic coordinates for a list of cities from a .yaml file and/or the corresponding database dictionary. We get city coordinates from the openstreetmap.org service. The resulting forecast data is stored in the posrgresql database for further processing with control of data loading via a .csv file.

Consists of the following modules:

config - module contains constants;
model - the module contains the data model in the database;
data_json_parser - the module is responsible for processing the received response;
data_save - module for writing received data to csv or to a database;
windydb_connector - the module is responsible for connecting to the database;
location_city - the module is responsible for obtaining geographic coordinates of cities for a request from the Openstreetmap.org service according to a list from a .yaml file or from the corresponding database dictionary;
run_windy.py(sh) - launcher module.
