import requests

class RequestSender:
    def __init__(self):
        pass

    def send_get_request(self, url, params=None):

        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка при выполнении GET запроса. Статус код: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при выполнении GET запроса: {e}")
            return None
