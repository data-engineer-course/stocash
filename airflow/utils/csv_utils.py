import time
import csv
import os
import boto3
from subprocess import PIPE, Popen
from utils import hvac_utils

aws_access_key_id = hvac_utils.read_vault('AWS_ACCESS_KEY_ID')
aws_secret_access_key = hvac_utils.read_vault('AWS_SECRET_ACCESS_KEY')

# for test
if not aws_access_key_id:
    aws_access_key_id = "pnPnSD6URaW1IyoB"
    aws_secret_access_key = "Vqz6yaOgvdfOw4RmJntFH1ksgqNK3C8v"

os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key

s3 = boto3.resource('s3', endpoint_url="http://127.0.0.1:9010")
hdfs_url = 'hdfs://localhost:9000'


def download(csv_file, csvreader, symbol):
    with open(f'./{csv_file}', 'w') as f:
        writer = csv.writer(f, dialect='excel')
        for idx, row in enumerate(csvreader):
            if idx == 0:
                row.append('symbol')
            else:
                row.append(symbol)
            writer.writerow(row)


def save_to_hdfs(csv_file, symbol):
    from_path = os.path.abspath(f'./{csv_file}')
    file_name = f'{round(time.time())}_{symbol}.csv'
    to_path = f'{hdfs_url}/bronze/{file_name}'
    print(f"from path {from_path}")
    print(f"to path {to_path}")
    put = Popen(["hadoop", "fs", "-put", from_path, to_path], stdin=PIPE, bufsize=-1)
    put.communicate()


def save_to_s3(csv_file, symbol):
    from_path = os.path.abspath(f'./{csv_file}')
    file_name = f'{round(time.time())}_{symbol}.csv'
    s3.Object('my-s3bucket', f'/bronze/{file_name}').put(Body=open(from_path, 'rb'))
