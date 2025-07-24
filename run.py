import time
from cashing.data_fetcher import fetch_data
from cashing.db_operations import cash_db, check_status


def update_db():
    """Обновляет базу данных, извлекая данные из API и очищая старые записи."""
    print("Запуск обновления базы данных...")
    cesar_result, wialon_result = fetch_data()
    print(f"Получил от Виалона: {len(wialon_result)}")
    print(f"Получил от Цезаря: {len(cesar_result)}")
    cash_db(cesar_result, wialon_result)
    print("Обновление базы данных завершено.")



if __name__ == "__main__":
    print("Запуск планировщика задач...")
    while True:
        if check_status() == 0:
            print('Модуль отключен. Ожидание 100 секунд')
            time.sleep(100)
        else:
            update_db()
            time.sleep(40)
