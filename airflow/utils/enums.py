from enum import Enum

class TimeSeriesInterval(Enum):
    INTRADAY = 1
    MONTHLY = 2


class SettingKeys(Enum):
    INTERVAL_MINUTES = 'interval_minutes'
    JAR_PATH = 'jar_path'
    SYMBOLS = 'symbols'
    OBJECT_STORAGE = 'object_storage'