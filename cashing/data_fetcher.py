# cashing/data_fetcher.py

import api_cesar_connector as CesarConnector
import api_wialon_connector as WialonConnector

def fetch_data():
    """Получает данные из API."""

    try:
        cesar_connector = CesarConnector.CesarApi()
        cesar_result = cesar_connector.get_cars_info()
    except Exception as e:
        print(f"Error occurred while fetching data from Cesar API: {e}")
        cesar_result = []

    try:
        wialon_connector = WialonConnector.WialonApi()
        print(wialon_connector.sid)
        wialon_result = wialon_connector.search_all_items() or []
    except Exception as e:
        print(f"Error occurred while fetching data from Wialon API: {e}")
        wialon_result = []


    if cesar_result is None or cesar_result == []:
        print("Cesar API returned no data")
    if wialon_result is None or wialon_result == []:
        print("Wialon API returned no data")

    return cesar_result or [], wialon_result or []
