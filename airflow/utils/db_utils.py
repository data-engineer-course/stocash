from clickhouse_driver import Client
from utils.enums import SettingKeys

client = Client(host='localhost', port=9001)

def read_settings():
    result = {
        SettingKeys.INTERVAL_MINUTES.value: 0,
        SettingKeys.JAR_PATH.value: '',
        SettingKeys.SYMBOLS.value: '',
        SettingKeys.OBJECT_STORAGE.value: ''
    }

    settings = client.execute("SELECT key, value FROM de.settings")
    for s in settings:
        result[s[0]] = s[1]

    return result