# cashing/utils.py

import time
from datetime import datetime


def to_unix_time(dt_str):
    """Преобразует строку даты в UNIX-время."""
    if dt_str is None:
        return 0
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
        return int(time.mktime(dt.timetuple()))
    except ValueError:
        print(f"Error parsing date: {dt_str}")
        return 0

def z_to_unix_time(timestamp_str):
    """Преобразует строку времени z в Unix timestamp."""
    try:
        if timestamp_str:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return int(dt.timestamp())
        return 0
    except Exception as e:
        print(f"Error parsing date: {timestamp_str}")
        return 0