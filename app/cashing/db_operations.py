import os
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from app.models import SystemSettings
from app.cashing.utils import to_unix_time, z_to_unix_time

# Настройка логгера (совместимого с scheduler.py и data_fetcher.py)
logger = logging.getLogger(__name__)

# Конфигурация базы данных
SQLALCHEMY_DATABASE_URL = os.getenv('SQLALCHEMY_DATABASE_URL', 'sqlite:///default.db')
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def bulk_insert_or_replace(session, query, params):
    """Выполняет REPLACE INTO в батчах для повышения производительности."""
    try:
        logger.debug(f"Выполнение батч-операции с {len(params)} записями.")
        session.execute(query, params)
        logger.info(f"Успешно выполнена батч-операция с {len(params)} записями.")
    except Exception as e:
        logger.error(f"Ошибка при выполнении батч-операции: {str(e)}")
        session.rollback()
        raise

def process_cesar_result(session, cesar_result):
    """Обрабатывает данные из cesar_result и выполняет REPLACE INTO cash_cesar."""
    if not cesar_result:
        logger.warning("Нет данных из Cesar для обработки.")
        return

    replace_query = text(
        """REPLACE INTO cash_cesar (unit_id, object_name, pin, vin, last_time, pos_x, pos_y, created_at, device_type)
           VALUES (:unit_id, :object_name, :pin, :vin, :last_time, :pos_x, :pos_y, :created_at, :device_type)"""
    )

    batch_data = []
    for item in cesar_result:
        if item is None:
            logger.warning("Пропущен элемент None в cesar_result.")
            continue

        if None in [item.get('unit_id'), item.get('object_name'), item.get('vin')]:
            logger.warning(f"Пропущен элемент из-за отсутствия обязательных полей: {item}")
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
        logger.info(f"Подготовлено {len(batch_data)} записей для cash_cesar.")
        bulk_insert_or_replace(session, replace_query, batch_data)
    else:
        logger.warning("Нет валидных данных для вставки в cash_cesar.")

def process_wialon_result(session, wialon_result):
    """Обрабатывает данные из wialon_result и обновляет cash_wialon, cash_history_wialon."""
    if not wialon_result:
        logger.warning("Нет данных из Wialon для обработки.")
        return

    replace_query = text(
        """REPLACE INTO cash_wialon (id, uid, nm, pos_x, pos_y, gps, last_time, last_pos_time, cmd, sens, valid_nav)
           VALUES (:id, :uid, :nm, :pos_x, :pos_y, :gps, :last_time, :last_pos_time, :cmd, :sens, :valid_nav)"""
    )

    batch_data = []
    for idx, item in enumerate(wialon_result):
        if item is None:
            logger.warning(f"Пропущен элемент None на индексе {idx} в wialon_result.")
            continue

        try:
            if item.get('id') is None or item.get('nm') is None:
                logger.warning(f"Пропущен элемент на индексе {idx} из-за отсутствия обязательных полей: {item}")
                continue

            uid = item.get('uid', 0)
            uid = 0 if not str(uid).isdigit() else uid
            nm = item.get('nm', '').split('|')[0].strip() if '|' in item.get('nm', '') else item.get('nm', '')

            cmd = {c['id']: c['n'] for c in item.get('cml', {}).values()} if item.get('cml') else {}
            sens = {s['id']: (s['n'], s['m']) for s in item.get('sens', {}).values()} if item.get('sens') else {}

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
                valid_nav = item.get('lmsg', {}).get('p', {}).get('valid_nav', 0) if item.get('lmsg') is not None else 0
            else:
                last_time = 0
                valid_nav = 0

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
                'sens': str(sens),
                'valid_nav': valid_nav,
            })

        except Exception as e:
            logger.error(f"Ошибка при обработке элемента на индексе {idx}: {str(e)}")
            continue

    if batch_data:
        logger.info(f"Подготовлено {len(batch_data)} записей для cash_wialon.")
        bulk_insert_or_replace(session, replace_query, batch_data)
    else:
        logger.warning("Нет валидных данных для вставки в cash_wialon.")

def process_axenta_result(session, axenta_result):
    """Обрабатывает данные из axenta_result и выполняет REPLACE INTO cash_axenta."""
    if not axenta_result:
        logger.warning("Нет данных из Axenta для обработки.")
        return

    replace_query = text(
        """REPLACE INTO cash_axenta (
            id, uid, nm, pos_x, pos_y, gps, last_time, last_pos_time, 
            connected_status, cmd, sens, valid_nav
        ) VALUES (
            :id, :uid, :nm, :pos_x, :pos_y, :gps, :last_time, :last_pos_time, 
            :connected_status, :cmd, :sens, :valid_nav
        )"""
    )

    batch_data = []
    for idx, item in enumerate(axenta_result):
        if item is None:
            logger.warning(f"Пропущен элемент None на индексе {idx} в axenta_result.")
            continue

        try:
            # Проверка обязательных полей
            if item.get('id') is None or item.get('name') is None:
                logger.warning(f"Пропущен элемент на индексе {idx} из-за отсутствия обязательных полей: {item}")
                continue

            # Извлечение имени без суффиксов после '|'
            nm = item.get('name', '').split('|')[0].strip() if '|' in item.get('name', '') else item.get('name', '')

            # Извлечение уникального идентификатора и проверка, что это число
            uid = item.get('uniqueId', '0')
            uid = 0 if not str(uid).isdigit() else int(uid)

            # Извлечение данных о последнем сообщении
            last_message = item.get('lastMessage', {})
            pos = last_message.get('pos', {})
            pos_x = pos.get('x', 0.0) if pos.get('x') is not None else 0.0
            pos_y = pos.get('y', 0.0) if pos.get('y') is not None else 0.0
            gps = pos.get('sc', 0) if pos.get('sc') is not None else 0
            last_time = z_to_unix_time(last_message.get('t', None))
            last_pos_time = z_to_unix_time(last_message.get('tpos', None))

            # Поля cmd и sens (пустые, так как в данных Axenta нет аналогов cml и sens из Wialon)
            cmd = ''
            sens = ''

            # valid_nav (устанавливаем 1 по умолчанию, так как в данных нет явного аналога)
            valid_nav = 1

            batch_data.append({
                'id': item.get('id'),
                'uid': uid,
                'nm': nm,
                'pos_x': pos_x,
                'pos_y': pos_y,
                'gps': gps,
                'last_time': last_time,
                'last_pos_time': last_pos_time,
                'connected_status': item.get('connectedStatus', False),
                'cmd': cmd,
                'sens': sens,
                'valid_nav': valid_nav
            })

        except Exception as e:
            logger.error(f"Ошибка при обработке элемента на индексе {idx}: {str(e)}")
            continue

    if batch_data:
        logger.info(f"Подготовлено {len(batch_data)} записей для cash_axenta.")
        bulk_insert_or_replace(session, replace_query, batch_data)
    else:
        logger.warning("Нет валидных данных для вставки в cash_axenta.")

def update_wialon_history_via_sql():
    """Вызов SQL-функции для обновления CashHistoryWialon."""
    session = SessionLocal()
    try:
        logger.info("Вызов SQL-функции update_cash_history_wialon.")
        session.execute(text("CALL update_cash_history_wialon"))
        session.commit()
        logger.info("Успешно обновлена история Wialon.")
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка при обновлении истории Wialon: {str(e)}")
    finally:
        session.close()

def update_cesar_history_via_sql():
    """Вызов SQL-функции для обновления CashHistoryCesar."""
    session = SessionLocal()
    try:
        logger.info("Вызов SQL-функции update_cash_history_cesar.")
        session.execute(text("CALL update_cash_history_cesar"))
        session.commit()
        logger.info("Успешно обновлена история Cesar.")
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка при обновлении истории Cesar: {str(e)}")
    finally:
        session.close()

def update_axenta_history_via_sql():
    """Вызов SQL-функции для обновления CashAxentaCesar."""
    session = SessionLocal()
    try:
        logger.info("Вызов SQL-функции update_cash_history_axenta.")
        session.execute(text("CALL update_cash_history_axenta"))
        session.commit()
        logger.info("Успешно обновлена история Axenta.")
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка при обновлении истории Axenta: {str(e)}")
    finally:
        session.close()

def cash_db(cesar_result, wialon_result, axenta_result):
    """Обновляет базу данных данными из cesar_result и wialon_result."""
    session = SessionLocal()
    try:
        logger.info("Начало обновления базы данных.")
        process_cesar_result(session, cesar_result)
        process_wialon_result(session, wialon_result)
        process_axenta_result(session, axenta_result)
        session.commit()
        logger.info("Успешно выполнен commit операций с базой данных.")
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка при выполнении операций с базой данных: {str(e)}")
        raise
    finally:
        session.close()
        logger.info("Обновление истории...")
        update_wialon_history_via_sql()
        update_cesar_history_via_sql()
        update_axenta_history_via_sql()

def check_status():
    """Проверяет статус системы в базе данных."""
    try:
        session = SessionLocal()
        result = session.query(SystemSettings).filter(SystemSettings.id == 0).first()
        session.close()
        status = result.enable_db_cashing if result else 0
        logger.debug(f"Статус системы: {'включен' if status else 'выключен'}.")
        return status
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {str(e)}")
        return 0