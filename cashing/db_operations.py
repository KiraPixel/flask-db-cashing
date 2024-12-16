from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from models import CashWialon, CashCesar, CashHistoryWialon
import config
from cashing.utils import to_unix_time
from datetime import datetime, timedelta

# Конфигурация базы данных
SQLALCHEMY_DATABASE_URL = config.SQLALCHEMY_DATABASE_URL
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def bulk_insert_or_replace(session, query, params):
    """Выполняет REPLACE INTO в батчах для повышения производительности."""
    try:
        session.execute(query, params)
    except Exception as e:
        print(f"Error during bulk operation: {e}")
        session.rollback()
        raise


def process_cesar_result(session, cesar_result):
    """Обрабатывает данные из cesar_result и выполняет REPLACE INTO cash_cesar."""
    if not cesar_result:
        return

    replace_query = text(
        """REPLACE INTO cash_cesar (unit_id, object_name, pin, vin, last_time, pos_x, pos_y, created_at, device_type)
           VALUES (:unit_id, :object_name, :pin, :vin, :last_time, :pos_x, :pos_y, :created_at, :device_type)"""
    )

    batch_data = []
    for item in cesar_result:
        # Проверка на None
        if item is None:
            print("Skipping None item")
            continue

        # Проверка на отсутствие ключей
        if None in [item.get('unit_id'), item.get('object_name'), item.get('vin')]:
            print(f"Skipping item due to missing required fields: {item}")
            continue

        object_name = item.get('object_name', '').split('|')[0].strip() if '|' in item.get('object_name', '') else item.get('object_name', '')
        batch_data.append({
            'unit_id': item.get('unit_id'),
            'object_name': object_name,
            'pin': item.get('pin'),
            'vin': item.get('vin'),
            'last_time': to_unix_time(item.get('receive_time', None)),
            'pos_x': item.get('lat', 0.0),
            'pos_y': item.get('lon', 0.0),
            'created_at': to_unix_time(item.get('created_at', None)),
            'device_type': item.get('device_type', 'Unknown')
        })

    if batch_data:
        bulk_insert_or_replace(session, replace_query, batch_data)


def process_wialon_result(session, wialon_result):
    """Обрабатывает данные из wialon_result и обновляет cash_wialon, cash_history_wialon."""
    if not wialon_result:
        return

    replace_query = text(
        """REPLACE INTO cash_wialon (id, uid, nm, pos_x, pos_y, gps, last_time, last_pos_time, cmd, sens)
           VALUES (:id, :uid, :nm, :pos_x, :pos_y, :gps, :last_time, :last_pos_time, :cmd, :sens)"""
    )

    batch_data = []
    for idx, item in enumerate(wialon_result):
        if item is None:
            print(f"Skipping None item at index {idx}")
            continue

        try:
            # Проверяем наличие необходимых ключей
            if item.get('id') is None or item.get('nm') is None:
                print(f"Skipping item due to missing required fields at index {idx}: {item}")
                continue

            uid = item.get('uid', 0)
            uid = 0 if not str(uid).isdigit() else uid
            nm = item.get('nm', '').split('|')[0].strip() if '|' in item.get('nm', '') else item.get('nm', '')

            # Проверяем наличие sens и cml, создаем пустые словари, если они отсутствуют
            cmd = {c['id']: c['n'] for c in item.get('cml', {}).values()} if item.get('cml') else {}
            sens = {s['id']: s['n'] for s in item.get('sens', {}).values()} if item.get('sens') else {}

            # Обработка pos
            if item.get('pos') is not None:
                pos_x = item['pos'].get('x', 0.0) if item['pos'].get('x') is not None else 0.0
                pos_y = item['pos'].get('y', 0.0) if item['pos'].get('y') is not None else 0.0
                gps = item.get('pos', {}).get('sc', 0) if item.get('pos') is not None else 0
                last_pos_time = item.get('pos', {}).get('t', 0) if item.get('pos') is not None else 0
            else:
                pos_x = 0.0
                pos_y = 0.0
                gps = -1
                last_pos_time = 0

            if item.get('lmsg') is not None:
                last_time = item.get('lmsg', {}).get('t', 0) if item.get('lmsg') is not None else 0
            else:
                last_time = 0

            batch_data.append({
                'id': item.get('id'),
                'uid': uid,
                'nm': nm,
                'pos_x': pos_x,
                'pos_y': pos_y,
                'gps': gps,
                'last_time': last_time,
                'last_pos_time': last_pos_time,
                'cmd': str(cmd),
                'sens': str(sens)
            })

        except Exception as e:  # Ловим все исключения
            print(f"Error processing item at index {idx}: {item}")
            print(f"Exception: {e}")
            continue  # Пропустить ошибочный элемент

    if batch_data:
        bulk_insert_or_replace(session, replace_query, batch_data)


def update_wialon_history_via_sql():
    """Вызов SQL-функции для обновления CashHistoryWialon."""
    session = SessionLocal()
    try:
        # Вызов SQL-функции с явным объявлением как текстового запроса
        result = session.execute(text("CALL update_cash_history_wialon();")).fetchone()

        # Получаем количество добавленных строк из возвращенного результата
        added_rows = result[0] if result else 0

        # Выводим количество добавленных строк
        print(f"Количество добавленных строк в cash_history_wialon: {added_rows}")

    except Exception as e:
        session.rollback()
        print(f"Error in update_wialon_history_via_sql: {e}")
    finally:
        session.close()


def cash_db(cesar_result, wialon_result):
    session = SessionLocal()
    try:
        process_cesar_result(session, cesar_result)
        process_wialon_result(session, wialon_result)
        session.commit()  # Один общий commit для всех операций
    except Exception as e:
        session.rollback()
        print(f"Error occurred during database operation: {e}")
    finally:
        session.close()
        print('Обновляю историю...')
        update_wialon_history_via_sql()
