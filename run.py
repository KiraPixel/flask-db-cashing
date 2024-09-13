import time
import schedule
from cashing.data_fetcher import fetch_data
from cashing.db_operations import clear_db, cash_db
from cashing.transport_updater import update_transport
import system_status_manager


def update_db():
    """Обновляет базу данных, извлекая данные из API и очищая старые записи."""
    print("Запуск обновления базы данных...")
    cesar_result, wialon_result = fetch_data()
    system_status_manager.set_status('db', True)
    clear_db()
    cash_db(cesar_result, wialon_result)
    system_status_manager.set_status('db', False)
    print("Обновление базы данных завершено.")


#update_db()

# Расписание задач
schedule.every(1).minutes.do(update_db)  # Выполнять update_db() каждую минуту

if __name__ == "__main__":
    print("Запуск планировщика задач...")
    while True:
        schedule.run_pending()
        time.sleep(1)  # Задержка, чтобы не перегружать процессор
