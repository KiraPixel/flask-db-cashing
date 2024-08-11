# cashing/utils.py

import datetime
import time


def to_unix_time(dt_str):
    """Преобразует строку даты в UNIX-время."""
    if dt_str is None:
        return 0
    try:
        dt = datetime.datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
        return int(time.mktime(dt.timetuple()))
    except ValueError:
        print(f"Error parsing date: {dt_str}")
        return 0
