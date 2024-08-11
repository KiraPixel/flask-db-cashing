# cashing/data_fetcher.py

import api_cesar_connector as CesarConnector
import api_wialon_connector as WialonConnector

def fetch_data():
    """Получает данные из API."""
    cesar_connector = CesarConnector.CesarApi()
    wialon_result = WialonConnector.search_all_items()

    try:
        cesar_result = cesar_connector.get_cars_info()
    except Exception as e:
        print(f"Error occurred while fetching data from Cesar API: {e}")
        cesar_result = []

    if cesar_result is None:
        print("Cesar API returned no data")
    if wialon_result is None:
        print("Wialon API returned no data")

    return cesar_result or [], wialon_result or []
