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


def update_transport_status():
    print("Запуск обновления транспорта...")
    system_status_manager.set_status('transport', True)
    update_transport()
    system_status_manager.set_status('transport', False)
    print("Обновление транспорта завершено.")


def run_transport_update():
    """Вызываем обновление транспорта только в нужное время"""
    current_minute = time.localtime().tm_min
    if current_minute % 10 == 0:
        update_transport_status()



# Расписание задач
schedule.every(1).minutes.do(update_db)  # Выполнять update_db() каждую минуту

#schedule.every(1).minute.do(run_transport_update)  # Проверяем каждую минуту, если время для обновления транспорта

if __name__ == "__main__":
    print("Запуск планировщика задач...")
    while True:
        schedule.run_pending()
        time.sleep(1)  # Задержка, чтобы не перегружать процессор
