import pandas as pd
import time

class JsonParser:

    def __init__(self):
        pass

    def parse_windy_json(self, json_data):
        """
        Функция для разбора JSON-ответа от Windy.com и формирования DataFrame.
        """

        data_keys = list(json_data.keys())
        df = pd.DataFrame()

        # столбец с временными метками
        try:
            timestamps = pd.to_datetime(json_data['ts'], unit='ms')
        except ValueError:
            print("Ошибка: Неверный формат timestamp.")
            return None

        # 1 предстоящие сутки
        tomorrow = int(time.time() + 1 * 86400)

        data_rows = []  # cписок для DataFrame

        # для каждого timestamp:
        for i in range(len(json_data['ts'])):
            timestamp_str = str(json_data['ts'][i])[:10]
            if time.strftime("%Y-%m-%d", time.localtime(int(timestamp_str))) == time.strftime("%Y-%m-%d",
                                                                                              time.localtime(tomorrow)):
                data_row = {'timestamp': timestamps[i]}
                for key in data_keys:
                    if key in ["ts", "units", "warning"]:
                        continue
                    data_row[key] = json_data[key][i]
                data_rows.append(data_row)

        df = pd.concat([df, pd.DataFrame(data_rows)])

        if "units" in json_data:
            df.attrs["units"] = json_data["units"]

        # Заменяем дефисы в именах столбцов на нижнее подчеркивание
        df.columns = df.columns.str.replace('-', '_')

        return df