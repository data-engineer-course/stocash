CREATE OR REPLACE TABLE de.time_series_unique (
    timestamp DateTime, 
    open Decimal32(4), 
    high Decimal32(4), 
    low Decimal32(4), 
    close Decimal32(4), 
    volume Int32,
    symbol String) 
ENGINE = ReplacingMergeTree
ORDER BY (timestamp, symbol);

INSERT INTO de.time_series_unique
(`timestamp`, `open`, high, low, `close`, volume, symbol)
SELECT `timestamp`, `open`, high, low, `close`, volume, symbol FROM de.time_series;

CREATE OR REPLACE VIEW de.vw_time_series  AS
SELECT `timestamp`, `open`, high, low, `close`, volume, symbol
FROM de.time_series_unique;
