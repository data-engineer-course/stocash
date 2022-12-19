# Анализ рынка валют

## Общая задача
Создать ETL-процесс формирования витрин данных для анализа изменений курса валют.

<details>
  <summary>Подробное описание задачи</summary>

Разработать скрипты загрузки данных в 2-х режимах:
- Инициализирующий – загрузка полного слепка данных источника
- Инкрементальный – загрузка дельты данных за прошедшие сутки

Организовать правильную структуру хранения данных

- Сырой слой данных
- Промежуточный слой
- Слой витрин

В качестве результата работы программного продукта необходимо написать скрипт, который формирует витрину данных следующего содержания

- Название валюты
- Суммарный объем торгов за последние сутки
- Курс валюты на момент открытия торгов для данных суток
- Курс валюты на момент закрытия торгов для данных суток
- Разница(в %) курса с момента открытия до момента закрытия торгов для данных суток
- Минимальный временной интервал, на котором был зафиксирован самый крупный объем торгов для данных суток
- Минимальный временной интервал, на котором был зафиксирован максимальный курс для данных суток
- Минимальный временной интервал, на котором был зафиксирован минимальный курс торгов для данных суток

**Дополнение**:

В качестве основы витрины необходимо выбрать 5-10 различных валют или акций компаний.

**Источники**:

https://www.alphavantage.co/
</details>



## Установка окружения

Всё окружение будет устанавливаться на локальной либо на виртуальной машине. В моем случае это

- [Ubuntu 22.04 LST](https://ubuntu.com/tutorials/install-ubuntu-desktop#1-overview)
- [Hadoop 3.2.1](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation) - нужно для организации озера данных
- [Airflow 2.5.0](https://airflow.apache.org/docs/apache-airflow/stable/start.html) - удобен тем, что в нём можно увидеть графический DAG и писать код на Python
- [Spark 3.3.1](https://spark.apache.org/downloads.html) - быстрая обработка данных
- [ClickHouse 22.11.2](./docker/clickhouse) - быстро делает выборки. Это пригодится для представления vw_time_series, где берутся только уникальные строки


## Схема работы

![График1](images/dag.png)

1. **read_settings** - чтение акций, которые необходимо загрузить и других настроек
1. **branch_operator** - проверяет значение переменной *time_series_interval*. Если она равна INTRADAY, то будет чтение за один день, в противном случае за месяц.
1. **download_intraday_time_series** - чтение данных за один день из Alpha Vantage в ClickHouse, и параллельное сохранение бэкапов как json дампы в HDFS папку '/bronze' (на всякий случай)
1. **download_monthly_time_series** - чтение данных за один месяц из Alpha Vantage в ClickHouse, и параллельное сохранение бэкапов как json дампы в HDFS папку '/bronze' (на всякий случай)
1. **run_spark** - Spark читает эти данные из ClickHouse, делает преобразования, строит витрину, результат пишетв виде parquet файла в HDFS папку '/gold'
1. **success** - сообщение об успешном выполнении задания


## Структура БД
- **symbols** - таблица с валютами и акциями
- **time_series** - таблица с каждодневной информацией
- **vw_time_series** - представление с уникальными строками из time_series
- **settings** - различные настройки приложения

![График1](images/er.png)


## HDFS

### Инициализация

```bash
hdfs namenode -format
start-dfs.sh
hdfs dfs -mkdir /bronze
hdfs dfs -mkdir /gold
hdfs dfsadmin -safemode leave  
# если нужно остановить, то stop-dfs.sh
```

### Структура

```bash
├── bronze      # сырые данные в виде json файлов
└── gold        # готовые витрины в виде parquet файлов
```


## Airflow

**pip** пакеты для работы DAG
- clickhouse_driver
- apache-airflow
- alpha_vantage

<details>
  <summary>Инициализация</summary>

```bash
# Airflow needs a home. `~/airflow` is the default, but you can put it
# somewhere else if you prefer (optional)
export AIRFLOW_HOME=~/airflow

# Install Airflow using the constraints file
AIRFLOW_VERSION=2.5.0
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.7
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# set your key
export ALPHAVANTAGE_KEY=...

# The Standalone command will initialise the database, make a user,
# and start all components for you.
airflow standalone

# Visit localhost:8080 in the browser and use the admin account details
# shown on the terminal to login.
# Enable the example_bash_operator dag in the home page
```
</details>