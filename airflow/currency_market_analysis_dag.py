from datetime import datetime, date
import time
import os
import csv
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
time_series = TimeSeries(key=os.environ["ALPHAVANTAGE_KEY"], output_format='csv')


class TimeSeriesInterval(Enum):
    INTRADAY = 1
    MONTHLY = 2


class SettingKeys(Enum):
    INTERVAL_MINUTES = 'interval_minutes'
    JAR_PATH = 'jar_path'
    SYMBOLS = 'symbols'


def read_settings():
    interval_minutes = 0
    jar_path = ''
    symbols = ''

    settings = client.execute("SELECT key, value FROM de.settings")
    for s in settings:
        if s[0] == SettingKeys.INTERVAL_MINUTES.value:
            interval_minutes = s[1]
        if s[0] == SettingKeys.JAR_PATH.value:
            jar_path = s[1]
        if s[0] == SettingKeys.SYMBOLS.value:
            symbols = s[1]

    return {
        SettingKeys.INTERVAL_MINUTES.value: interval_minutes,
        SettingKeys.JAR_PATH.value: jar_path,
        SettingKeys.SYMBOLS.value: symbols
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
    # извлекаем список валют/акций
    settings = ti.xcom_pull(task_ids='read_settings')
    symbols = settings[SettingKeys.SYMBOLS.value].split(",")
    print(f'symbols: {symbols}')

    for symbol in symbols:
        data, meta_data = {}, {}

        if interval == TimeSeriesInterval.INTRADAY:
            print(f'{settings[SettingKeys.INTERVAL_MINUTES.value]}min interval')
            data, meta_data = time_series.get_intraday(symbol,
                                                       interval=f'{settings[SettingKeys.INTERVAL_MINUTES.value]}min')

            # т.к. Alpha Vantage API не позволяет указать конкретный день,
            # то тут можно дополнительно фильтровать данные за прошлый день
            data = filter_dates(data)

        if interval == TimeSeriesInterval.MONTHLY:
            data, meta_data = time_series.get_monthly(symbol)

        csv_file = "data.csv"

        download_csv(csv_file, data, symbol)

        save_csv_to_hdfs(csv_file, symbol)


def filter_dates(data):
    result = []
    for idx, row in enumerate(data):
        if idx == 0:
            result.append(row)
        else:
            date_object = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S').date()
            today = date.today()
            delta = today - date_object
            if delta.days == 1:
                result.append(row)

    return result


def download_csv(csv_file, csvreader, symbol):
    with open(f'./{csv_file}', 'w') as f:
        writer = csv.writer(f, dialect='excel')
        for idx, row in enumerate(csvreader):
            if idx == 0:
                row.append('symbol')
            else:
                row.append(symbol)
            writer.writerow(row)


def save_csv_to_hdfs(csv_file, symbol):
    from_path = os.path.abspath(f'./{csv_file}')
    to_path = f'hdfs://localhost:9000/bronze/{round(time.time())}_{symbol}.csv'
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
