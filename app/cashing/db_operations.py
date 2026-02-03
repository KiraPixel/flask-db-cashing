import os
import logging
from http.cookiejar import uppercase_escaped_char

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
        object_name.upper()
        print(object_name)
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
            # logger.info(f'Парсим item {idx}: {item}')
            # Проверка обязательных полей
            if item.get('id') is None or item.get('name') is None:
                logger.warning(f"Пропущен элемент на индексе {idx} из-за отсутствия обязательных полей: {item}")
                continue

            # Извлечение имени без суффиксов после '|'
            nm = item.get('name', '').split('|')[0].strip() if '|' in item.get('name', '') else item.get('name', '')
            nm = nm.upper()

            # Извлечение уникального идентификатора и проверка, что это число
            uid = item.get('uniqueId', '0')
            uid = 0 if not str(uid).isdigit() else int(uid)

            # Извлечение данных о последнем сообщении
            pos_x = None
            pos_y = None
            gps = None
            last_time = 0
            last_pos_time = 0
            valid_nav = 0
            last_message = item.get('lastMessage', None)
            if last_message is not None:
                pos = last_message.get('pos', None)
                last_time = z_to_unix_time(last_message.get('t', None))
                last_pos_time = z_to_unix_time(last_message.get('tpos', None))
                if pos is not None:
                    pos_x = pos.get('x', 0.0) if pos.get('x') is not None else 0.0
                    pos_y = pos.get('y', 0.0) if pos.get('y') is not None else 0.0
                    gps = pos.get('sc', 0) if pos.get('sc') is not None else 0
                    if gps is not None and gps > 4:
                        valid_nav = 1

            # Поля cmd и sens (пустые, так как в данных Axenta нет аналогов cml и sens из Wialon)
            cmd = ''
            sens = ''

            batch_data.append({
                'id': item.get('id'),
                'uid': uid,
                'nm': nm,
                'pos_x': pos_y,
                'pos_y': pos_x,
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

def cash_db(cesar_result, axenta_result):
    """Обновляет базу данных данными из cesar_result и axenta_result."""
    session = SessionLocal()
    try:
        logger.info("Начало обновления базы данных.")
        process_cesar_result(session, cesar_result)
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