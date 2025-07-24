import json
import os
import time
import requests
from typing import Optional, Dict, List, Any


class WialonApi:
    _instance = None

    def __new__(cls, *args, **kwargs):
        """Реализация Singleton: возвращает существующий экземпляр или создаёт новый."""
        if cls._instance is None:
            cls._instance = super(WialonApi, cls).__new__(cls)
        return cls._instance

    def __init__(self, token: str = None, api_url: str = None):
        # Инициализация вызывается только один раз
        if not hasattr(self, '_initialized'):
            self.token = token or os.getenv('WIALON_TOKEN', 'default_token')
            self.api_url = api_url or os.getenv('WIALON_HOST', 'default_host')
            self.sid = None
            self.sid_expiry = 0  # Время истечения SID
            self.sid_lifetime = 300  # 5 минут в секундах
            self._initialized = True

    def get_wialon_sid(self) -> Optional[str]:
        """Получает новый SID от Wialon API."""
        params = {'token': self.token}
        try:
            response = requests.get(self.api_url, params={
                'svc': 'token/login',
                'params': json.dumps(params)
            }, verify=True)
            response.raise_for_status()
            result = response.json()
            if 'eid' in result:
                self.sid = result['eid']
                self.sid_expiry = time.time() + self.sid_lifetime
                return self.sid
            else:
                print(f"Error: {result}")
                return None
        except requests.RequestException as e:
            print(f"Error getting SID: {e}")
            return None

    def is_sid_valid(self) -> bool:
        """Проверяет, действителен ли текущий SID."""
        return self.sid is not None and time.time() < self.sid_expiry

    def ensure_sid(self) -> Optional[str]:
        """Гарантирует наличие действительного SID."""
        if not self.is_sid_valid():
            return self.get_wialon_sid()
        return self.sid

    def make_request(self, svc: str, params: Dict, retries: int = 1) -> Optional[Dict]:
        """Выполняет запрос к Wialon API с обработкой ошибок и перегенерацией SID."""
        sid = self.ensure_sid()
        if not sid:
            print("Failed to get valid SID")
            return None

        for attempt in range(retries + 1):
            try:
                response = requests.get(self.api_url, params={
                    'svc': svc,
                    'params': json.dumps(params),
                    'sid': sid
                }, verify=True)
                response.raise_for_status()
                result = response.json()

                if isinstance(result, dict) and 'error' in result:
                    error_code = result.get('error')
                    if error_code == 1003:  # SID истёк
                        if attempt < retries:
                            self.sid = None  # Сброс SID
                            sid = self.ensure_sid()
                            if not sid:
                                print("Failed to refresh SID")
                                return None
                            continue
                    print(f"Wialon API error: {result}")
                    return None
                return result
            except requests.RequestException as e:
                print(f"Request error: {e}")
                if attempt < retries:
                    self.sid = None  # Сброс SID
                    sid = self.ensure_sid()
                    if not sid:
                        print("Failed to refresh SID")
                        return None
                else:
                    print(f"Failed after {retries + 1} attempts")
                    return None

    def search_all_items(self) -> Optional[List[Dict]]:
        """Получает список всех объектов."""
        params = {
            'spec': {
                'itemsType': 'avl_unit',
                'propName': 'sys_name',
                'propValueMask': '*',
                'sortType': 'sys_name'
            },
            'force': 1,
            'flags': 1 | 256 | 1024 | 4096 | 524288,
            'from': 0,
            'to': 0
        }
        result = self.make_request('core/search_items', params)
        return result.get('items') if result else None

    def exec_cmd(self, unit_id: str) -> Optional[Dict]:
        """Выполняет команду для объекта."""
        params = {
            'itemId': unit_id,
            'commandName': "Включить",
            'linkType': '',
            'param': '',
            'timeout': 5,
            'flags': 0
        }
        return self.make_request('unit/exec_cmd', params)

    def get_sensors(self, unit_id: str) -> Optional[Dict]:
        """Получает данные датчиков для объекта."""
        params = {
            'unitId': unit_id,
            'sensors': ''
        }
        return self.make_request('unit/calc_last_message', params)