import time
import csv
import os
import boto3
from subprocess import PIPE, Popen

# for test
os.environ['AWS_ACCESS_KEY_ID'] = ""
os.environ['AWS_SECRET_ACCESS_KEY'] = ""

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
