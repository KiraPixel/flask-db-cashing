import json
import os
import time
from http.client import responses

import requests
from typing import Optional, Dict, List, Any



class AxentaApi:
    _instance = None

    def __new__(cls, *args, **kwargs):
        """Реализация Singleton: возвращает существующий экземпляр или создаёт новый."""
        if cls._instance is None:
            cls._instance = super(AxentaApi, cls).__new__(cls)
        return cls._instance

    def __init__(self, token: str = None, api_url: str = None):
        # Инициализация вызывается только один раз
        if not hasattr(self, '_initialized'):
            self.login = token or os.getenv('AXENTA_USERNAME', 'default_login')
            self.password = token or os.getenv('AXENTA_PASSWORD', 'default_password')
            self.api_url = api_url or os.getenv('AXENTA_HOST', 'default_host')
            self.token = None
            self.token_expiry = 0  # Время истечения SID
            self.token_lifetime = 600  # 10 минут в секундах
            self._initialized = False


    def get_axenta_token(self) -> Optional[str]:
        """Получает новый SID от Wialon API."""
        data = {
            'username': self.login,
            'password': self.password
        }
        try:
            response = requests.post(self.api_url+'auth/login/', data=data)
            result = response.json()
            if 'token' in result:
                self.token = result['token']
                self.token_expiry = time.time() + self.token_lifetime
                return self.token
            else:
                print(f"Error: {result}")
                return None
        except requests.RequestException as e:
            print(f"Error getting Token: {e}")
            return None

    def is_token_valid(self) -> bool:
        """Проверяет, действителен ли текущий SID."""
        return self.token is not None and time.time() < self.token_expiry

    def ensure_token (self) -> Optional[str]:
        """Гарантирует наличие действительного SID."""
        if not self.is_token_valid():
            return self.get_axenta_token()
        return self.token

    def make_request(self, method: str, uri: str, data : dict, retries: int = 1) -> Optional[Dict]:
        token = self.ensure_token()
        if not token:
            print('Failed to get token')
            return None

        for attempt in range(retries + 1):
            try:
                if method == 'GET':
                    response = requests.get(self.api_url + uri, data=data, headers={'Authorization': f'Token {token}'})
                elif method == 'POST':
                    response = requests.post(self.api_url + uri, data=data, headers={'Authorization': f'Token {token}'})
                else:
                    return None
                result = response.json()
                if response.status_code == 200:
                    return result
                else:
                    f'Произошла ошибка в запросе axenta {result}'
            except requests.RequestException as e:
                print(f"Axenta error: {e}")

    def search_all_items(self) -> Optional[List[Dict]]:
        result = self.make_request('GET', 'objects', None, retries=1)
        return result

    def exec_cmd(self, unit_id: str, cmd: dict ) -> bool:
        # todo
        result = self.make_request('POST', f'objects/{unit_id}/send_command', cmd)
        if result:
            return True
        else:
            return False

    def get_sensors(self, unit_id: str) -> Optional[Dict[str, Any]]:
        result = self.make_request('GET', f'objects/{unit_id}/sensors', None)
        return result

    def get_cmd(self, unit_id: str) -> Optional[Dict[str, Any]]:
        result = self.make_request('GET', f'objects/{unit_id}/commands', None)
        return result