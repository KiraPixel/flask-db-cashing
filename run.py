import time
from cashing.data_fetcher import fetch_data
from cashing.db_operations import cash_db


def update_db():
    """Обновляет базу данных, извлекая данные из API и очищая старые записи."""
    print("Запуск обновления базы данных...")
    cesar_result, wialon_result = fetch_data()
    cash_db(cesar_result, wialon_result)
    print("Обновление базы данных завершено.")



if __name__ == "__main__":
    print("Запуск планировщика задач...")
    while True:
        update_db()
        time.sleep(1)  # Задержка, чтобы не перегружать процессор
