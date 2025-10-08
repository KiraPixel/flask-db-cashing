import logging
from app import api_cesar_connector as CesarConnector, api_wialon_connector as WialonConnector, api_axenta_connector as AxentaConnector

logger = logging.getLogger(__name__)

def fetch_data():
    """Получает данные из API."""
    cesar_result = []
    wialon_result = []
    axenta_result = []
    try:
        logger.info("Получение данных из Cesar API...")
        cesar_connector = CesarConnector.CesarApi()
        cesar_result = cesar_connector.get_cars_info() or []
        logger.info(f"Успешно получено {len(cesar_result)} записей из Cesar API.")
    except Exception as e:
        logger.error(f"Ошибка при получении данных из Cesar API: {str(e)}")

    try:
        logger.info("Получение данных из Wialon API...")
        wialon_connector = WialonConnector.WialonApi()
        wialon_result = wialon_connector.search_all_items() or []
        logger.info(f"Успешно получено {len(wialon_result)} записей из Wialon API.")
    except Exception as e:
        logger.error(f"Ошибка при получении данных из Wialon API: {str(e)}")

    try:
        logger.info("Получение данных из Axenta API...")
        axenta_connector = AxentaConnector.AxentaApi()
        axenta_result = axenta_connector.search_all_items() or []
        logger.info(f"Успешно получено {len(axenta_result)} записей из Axenta API.")
    except Exception as e:
        logger.error(f"Ошибка при получении данных из Axenta API: {str(e)}")

    if not cesar_result:
        logger.warning("Cesar API не вернул данных.")
    if not wialon_result:
        logger.warning("Wialon API не вернул данных.")
    if not axenta_result:
        logger.warning("Axenta API не вернула данных.")

    return cesar_result, wialon_result, axenta_result