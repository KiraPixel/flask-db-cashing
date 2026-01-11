import time
import logging
from app.cashing.data_fetcher import fetch_data
from app.cashing.db_operations import cash_db, check_status


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def update_db():
    """Обновляет базу данных, извлекая данные из API и очищая старые записи."""
    try:
        logger.info("Запуск обновления базы данных...")
        cesar_result, axenta_result = fetch_data()
        cash_db(cesar_result, axenta_result)
        logger.info("Обновление базы данных завершено.")
    except Exception as e:
        logger.error(f"Ошибка при обновлении базы данных: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("Запуск планировщика задач...")
    while True:
        try:
            if check_status() == 0:
                logger.warning("Модуль отключен. Ожидание 100 секунд.")
                time.sleep(100)
            else:
                update_db()
                logger.info("Ожидание 10 секунд перед следующей итерацией.")
                time.sleep(10)
        except Exception as e:
            logger.error(f"Ошибка в главном цикле: {str(e)}")
            logger.info("Ожидание 60 секунд перед повторной попыткой.")
            time.sleep(60)