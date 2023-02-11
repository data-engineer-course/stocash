import hvac

client = hvac.Client(
    url='http://127.0.0.1:8200',
    token='dev-only-token',
)

def read_vault(key):
    return client.secrets.kv.read_secret_version(path='KeyName')['data']['data'][key]