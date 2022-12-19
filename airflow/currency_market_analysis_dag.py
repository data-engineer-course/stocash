from datetime import datetime
import time
import requests
import os
import json
from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from subprocess import PIPE, Popen

from airflow.utils.trigger_rule import TriggerRule
from clickhouse_driver import Client
from alpha_vantage.timeseries import TimeSeries
from enum import Enum

client = Client(host='localhost', port=9001)
time_series = TimeSeries(key=os.environ["ALPHAVANTAGE_KEY"])


class TimeSeriesInterval(Enum):
    INTRADAY = 1
    MONTHLY = 2


def read_settings():
    settings = client.execute("SELECT key, value FROM de.settings")
    for s in settings:
        if s[0] == 'interval_minutes':
            interval_minutes = s[1]
        if s[0] == 'jar_path':
            jar_path = s[1]

    result = client.execute('SELECT name FROM de.symbols')
    symbols = []
    for t in result:
        symbols.append(t[0])
    return {
        "interval_minutes": interval_minutes,
        "jar_path": jar_path,
        "symbols": ",".join(symbols)
    }


def python_branch():
    time_series_interval = TimeSeriesInterval.INTRADAY
    try:
        time_series_interval = TimeSeriesInterval[Variable.get("time_series_interval").upper()]
    except KeyError:
        Variable.set("time_series_interval", "INTRADAY")

    if time_series_interval == TimeSeriesInterval.INTRADAY:
        return "download_intraday_time_series"
    else:
        return "download_monthly_time_series"


def download_intraday_time_series(**kwargs):
    download_time_series(TimeSeriesInterval.INTRADAY, kwargs['ti'])


def download_monthly_time_series(**kwargs):
    download_time_series(TimeSeriesInterval.MONTHLY, kwargs['ti'])


def download_time_series(interval, ti):
    # получаем список акций
    settings = ti.xcom_pull(task_ids='read_settings')
    symbols = settings["symbols"].split(",")
    print(symbols)

    # создаем директорию
    directory = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
    put = Popen(["hadoop", "fs", "-mkdir", f"/bronze/{directory}"], stdin=PIPE, bufsize=-1)
    put.communicate()

    for symbol in symbols:
        data, meta_data = {}, {}

        if interval == TimeSeriesInterval.INTRADAY:
            print(f'{settings["interval_minutes"]}min interval')
            data, meta_data = time_series.get_intraday(symbol, interval=f'{settings["interval_minutes"]}min')

        if interval == TimeSeriesInterval.MONTHLY:
            data, meta_data = time_series.get_monthly(symbol)

        save_to_hdfs(directory, data, symbol)

        for key in data:
            datetime_object = datetime.strptime(key, '%Y-%m-%d %H:%M:%S')
            tms = time.mktime(datetime_object.timetuple())

            opn = float(data[key]['1. open'])
            hgh = float(data[key]['2. high'])
            lw = float(data[key]['3. low'])
            cls = float(data[key]['4. close'])
            vlm = int(data[key]['5. volume'])

            query = f"INSERT INTO de.time_series (timestamp, open, high, low, close, volume, symbol) VALUES ({tms}, {opn}, {hgh}, {lw}, {cls}, {vlm}, '{symbol}')"
            print(query)
            client.execute(query)


def save_to_hdfs(directory, data, symbol):
    with open('data.json', 'w') as fp:
        json.dump(data, fp)

    from_path = os.path.abspath('./data.json')
    to_path = f'hdfs://localhost:9000/bronze/{directory}/{symbol}.json'
    print(f"from path {from_path}")
    print(f"to path {to_path}")
    put = Popen(["hadoop", "fs", "-rm", to_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    put = Popen(["hadoop", "fs", "-put", from_path, to_path], stdin=PIPE, bufsize=-1)
    put.communicate()


def download_time_series_csv(**kwargs):
    ti = kwargs['ti']
    symbols = ti.xcom_pull(task_ids='read_settings')
    symbols = symbols.split(",")

    directory = datetime.now().strftime("%Y_%m_%d")
    put = Popen(["hadoop", "fs", "-mkdir", f"/bronze/{directory}"], stdin=PIPE, bufsize=-1)
    put.communicate()

    for symbol in symbols:
        CSV_URL = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={os.environ["ALPHAVANTAGE_KEY"]}&datatype=csv'

        with requests.Session() as s:
            with open('data.csv', 'wb') as f, requests.get(CSV_URL, stream=True) as r:
                for line in r.iter_lines():
                    f.write(line + '\n'.encode())

        from_path = os.path.abspath('./data.csv')
        to_path = f'hdfs://localhost:9000/bronze/{directory}/{symbol}.csv'
        print(f"from path {from_path}")
        print(f"to path {to_path}")

        put = Popen(["hadoop", "fs", "-put", from_path, to_path], stdin=PIPE, bufsize=-1)
        put.communicate()


with DAG(dag_id="currency_market_analysis_dag", start_date=datetime(2022, 1, 1), schedule="0 0 * * *",
         catchup=False) as dag:
    read_settings_python_task = PythonOperator(task_id="read_settings",
                                              python_callable=read_settings)

    choose_interval = BranchPythonOperator(task_id='branch_operator', python_callable=python_branch, do_xcom_push=False)

    download_intraday_time_series_python_task = PythonOperator(task_id="download_intraday_time_series",
                                                               python_callable=download_intraday_time_series,
                                                               do_xcom_push=False)

    download_monthly_time_series_python_task = PythonOperator(task_id="download_monthly_time_series",
                                                              python_callable=download_monthly_time_series,
                                                              do_xcom_push=False)

    spark_bash_task = BashOperator(task_id="run_spark",
                                   # bash_command="echo Running Spark...",
                                   bash_command="spark-submit --class org.example.App {{ ti.xcom_pull(task_ids='read_settings')['jar_path'] }}",
                                   do_xcom_push=False,
                                   trigger_rule=TriggerRule.ONE_SUCCESS)

    success_bash_task = BashOperator(task_id="success", bash_command="echo Success", do_xcom_push=False)

    read_settings_python_task >> choose_interval >> [download_intraday_time_series_python_task,
                                                    download_monthly_time_series_python_task] >> spark_bash_task >> success_bash_task
