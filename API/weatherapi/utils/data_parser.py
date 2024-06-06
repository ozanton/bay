class DataParser:
    def __init__(self, data):
        self.data = data

    def get_city(self):
        return self.data['location']['name']

    def get_temp_c(self):
        return self.data['current']['temp_c']

    def get_humidity(self):
        return self.data['current']['humidity']

    def get_wind_kph(self):
        return self.data['current']['wind_kph']

    def get_wind_degree(self):
        return self.data['current']['wind_degree']

    def get_last_updated(self):
        return self.data['current']['last_updated']

    def get_last_updated_epoch(self):
        return int(self.data['current']['last_updated_epoch'])

    def get_cloud(self):
        return self.data['current']['cloud']

    # up

    def get_precip(self):
        return self.data['current']['precip']

    def get_wind_speed(self):
        return self.data['current']['wind_speed']

    def get_wind_gust(self):
        return self.data['current']['wind_gust']

    def get_cloud_cover(self):
        return self.data['current']['cloud_cover']

    def get_uv_index(self):
        return self.data['current']['uv_index']

    def get_alerts(self):
        return self.data['current']['alerts']

    def get_feelslike_c(self):
        return self.data['current']['feelslike_c']

    def get_vis_km(self):
        return self.data['current']['vis_km']

    def get_uv(self):
        return self.data['current']['uv']
