CREATE DATABASE IF NOT EXISTS de;

CREATE OR REPLACE TABLE de.time_series (
    timestamp DateTime, 
    open Decimal32(4), 
    high Decimal32(4), 
    low Decimal32(4), 
    close Decimal32(4), 
    volume Int32,
    symbol String) 
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, symbol);

CREATE OR REPLACE VIEW de.vw_time_series  AS
SELECT `timestamp`, `open`, high, low, `close`, volume, symbol
FROM de.time_series
GROUP BY `timestamp`, `open`, high, low, `close`, volume, symbol;

CREATE OR REPLACE TABLE de.settings (key String, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO de.settings(key, value) VALUES ('symbols', 'IBM,USD,YNDX,AAPL,GOOGL,MSFT'), ('interval_minutes', '15'), ('jar_path', '/task2spark_2.12-0.1.0-SNAPSHOT.jar');
