from sqlalchemy.orm import sessionmaker
from sqlalchemy import update
from models import CashCesar, CashWialon, Transport
from cashing.db_operations import SessionLocal

def clean_transport_number(number):
    """Очистка и форматирование номера транспорта."""
    return number.strip().upper().replace(' ', '')

def clean_string_for_comparison(s):
    """Очистка строки для сравнения, удаление лишних пробелов и спецсимволов."""
    return ''.join(e for e in s.upper() if e.isalnum())

def update_transport():
    """Обновляет поле linked и equipment в таблице Transport, основываясь на данных из CashWialon и CashCesar."""
    session = SessionLocal()

    try:
        print("Starting update process...")

        # Сначала сбрасываем статус linked
        print("Resetting linked status in CashCesar and CashWialon...")
        session.query(CashCesar).update({CashCesar.linked: 0})
        session.query(CashWialon).update({CashWialon.linked: 0})
        session.commit()
        print("Reset complete.")

        # Получаем все транспорты
        print("Fetching all transports...")
        transports = session.query(Transport).all()
        transport_numbers = [clean_transport_number(transport.uNumber) for transport in transports]
        print(f"Fetched {len(transports)} transports.")

        # Создаем словари для хранения обновленных данных
        equipment_updates = {number: set() for number in transport_numbers}

        # Получаем все совпадения из CashCesar и CashWialon
        print("Fetching matches from CashCesar...")
        cesar_matches = session.query(CashCesar).all()
        print(f"Found {len(cesar_matches)} potential matches in CashCesar.")

        print("Fetching matches from CashWialon...")
        wialon_matches = session.query(CashWialon).all()
        print(f"Found {len(wialon_matches)} potential matches in CashWialon.")

        # Обрабатываем совпадения из CashCesar
        print("Processing matches from CashCesar...")
        for cesar in cesar_matches:
            cleaned_name = clean_string_for_comparison(cesar.object_name)
            for transport_number in transport_numbers:
                if transport_number in cleaned_name:
                    equipment_updates[transport_number].add(cesar.unit_id)
                    session.execute(update(CashCesar).where(CashCesar.unit_id == cesar.unit_id).values(linked=1))
                    break

        # Обрабатываем совпадения из CashWialon
        print("Processing matches from CashWialon...")
        for wialon in wialon_matches:
            cleaned_nm = clean_string_for_comparison(wialon.nm)
            for transport_number in transport_numbers:
                if transport_number in cleaned_nm:
                    equipment_updates[transport_number].add(wialon.id)
                    session.execute(update(CashWialon).where(CashWialon.id == wialon.id).values(linked=1))
                    break

        # Обновляем поле equipment у всех транспортов
        print("Updating equipment for all transports...")
        for transport in transports:
            transport_number = clean_transport_number(transport.uNumber)
            if transport_number in equipment_updates:
                transport.equipment = list(equipment_updates[transport_number])
            else:
                print(f"Warning: Transport number '{transport_number}' not found in equipment_updates.")

        session.commit()
        print("Transport table updated successfully.")

    except Exception as e:
        session.rollback()
        print(f"Error occurred while updating Transport: {e}")

    finally:
        session.close()
        print("Session closed.")
