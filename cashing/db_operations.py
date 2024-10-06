# cashing/db_operations.py

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models import CashWialon, CashCesar
import config
from cashing.utils import to_unix_time

# Конфигурация базы данных
SQLALCHEMY_DATABASE_URL = config.SQLALCHEMY_DATABASE_URL
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def clear_db():
    """Очищает таблицы CashCesar и CashWialon в базе данных."""
    session = SessionLocal()
    try:
        session.query(CashCesar).delete()
        session.query(CashWialon).delete()
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error occurred while clearing the database: {e}")
    finally:
        session.close()


def cash_db(cesar_result, wialon_result):
    """Добавляет данные в таблицы CashCesar и CashWialon."""
    session = SessionLocal()
    try:
        for item in cesar_result:
            if item is None:
                print("Received None item in cesar_result")
                continue

            unit_id = item.get('unit_id', None)
            object_name = item.get('object_name', None)
            pin = item.get('pin', None)
            vin = item.get('vin', None)
            last_time = to_unix_time(item.get('receive_time'))
            pos_x = item.get('lat', 0.0)
            pos_y = item.get('lon', 0.0)
            created_at = to_unix_time(item.get('created_at'))
            device_type = item.get('device_type', 'Unknown')

            if unit_id is None or object_name is None or vin is None:
                print(f"Skipping item due to missing required fields: {item}")
                continue

            cesar_entry = CashCesar(
                unit_id=unit_id,
                object_name=object_name,
                pin=pin,
                vin=vin,
                last_time=last_time,
                pos_x=pos_x,
                pos_y=pos_y,
                created_at=created_at,
                device_type=device_type
            )
            session.add(cesar_entry)

        for item in wialon_result:
            if item is None:
                print("Received None item in wialon_result")
                continue

            obj_id = item.get('id', None)
            uid = item.get('uid', None)
            nm = item.get('nm', None)
            if nm and '|' in nm:
                nm = nm.split('|')[0].strip()
            pos = item.get('pos', {}) if item.get('pos') else {}
            lmsg = item.get('lmsg', {}) if item.get('lmsg') else {}

            pos_x = pos.get('x', 0.0)
            pos_y = pos.get('y', 0.0)
            gps = pos.get('sc', 0)
            last_time = lmsg.get('t', 0)
            last_pos_time = pos.get('t', 0)
            cmd = item.get('cml', '')
            sens = item.get('sens', '')

            if obj_id is None or nm is None:
                print(f"Skipping item due to missing required fields: {item}")
                continue

            if cmd:
                cmd = {item["id"]: item["n"] for item in cmd.values()}

            if sens:
                sens = {item["id"]: item["n"] for item in sens.values()}

            try:
                uid = int(uid)
                if uid > 9223372036854775807:  # Пример для BIGINT в MySQL
                    print(f"Value for uid is too large: {nm} - {uid}. Replacing with 0.")
                    uid = 0
            except (ValueError, TypeError):
                print(f"Invalid value for uid: {nm} - {uid}. Replacing with 0.")
                uid = 0

            wialon_entry = CashWialon(
                id=obj_id,
                uid=uid,
                nm=nm,
                pos_x=pos_x,
                pos_y=pos_y,
                gps=gps,
                last_time=last_time,
                last_pos_time=last_pos_time,
                cmd=cmd,
                sens=sens,
            )
            session.add(wialon_entry)

        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error occurred while updating the database: {e}")
    finally:
        session.close()
