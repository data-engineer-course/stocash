import os
from datetime import datetime, date
from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from alpha_vantage.timeseries import TimeSeries
from utils.enums import TimeSeriesInterval, SettingKeys
from utils import db_utils as db, csv_utils as csv, hvac_utils


alphavantage_key = hvac_utils.read_vault('ALPHAVANTAGE_KEY')
time_series = TimeSeries(key=alphavantage_key, output_format='csv')


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
    # retrieve list of currencies
    settings = ti.xcom_pull(task_ids='read_settings')
    symbols = settings[SettingKeys.SYMBOLS.value].split(",")
    print(f'symbols: {symbols}')

    for symbol in symbols:
        data, meta_data = [], None

        if interval == TimeSeriesInterval.INTRADAY:
            print(f'{settings[SettingKeys.INTERVAL_MINUTES.value]}min interval')
            data, meta_data = time_series.get_intraday(symbol,
                                                       interval=f'{settings[SettingKeys.INTERVAL_MINUTES.value]}min')

            # because Alpha Vantage API does not allow you to specify a specific day, 
            # here you can additionally filter the data for the past day
            data = filter_dates(data)

        if interval == TimeSeriesInterval.MONTHLY:
            data, meta_data = time_series.get_monthly(symbol)

        csv_file = "data.csv"

        csv.download(csv_file, data, symbol)

        if settings[SettingKeys.OBJECT_STORAGE.value].lower == 'hdfs':
            csv.save_to_hdfs(csv_file, symbol)
        else:
            csv.save_to_s3(csv_file, symbol)


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


with DAG(dag_id="currency_market_analysis_dag", start_date=datetime(2022, 1, 1), schedule="0 0 * * *",
         catchup=False) as dag:
    read_settings_python_task = PythonOperator(task_id="read_settings",
                                               python_callable=db.read_settings)

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
