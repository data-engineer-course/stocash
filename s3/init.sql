CREATE OR REPLACE TABLE de.time_series_s3 (
    timestamp DateTime, 
    open Decimal32(4), 
    high Decimal32(4), 
    low Decimal32(4), 
    close Decimal32(4), 
    volume Int32,
    symbol String) 
ENGINE=S3('http://127.0.0.1:9010/my-s3bucket/bronze/*', 'Access Key', 'Secret Key', 'CSVWithNames');

ALTER TABLE de.`settings` UPDATE `key`='object_storage', value='s3';

CREATE OR REPLACE VIEW de.vw_time_series  AS
SELECT `timestamp`, `open`, high, low, `close`, volume, symbol
FROM de.time_series_s3
GROUP BY `timestamp`, `open`, high, low, `close`, volume, symbol;

