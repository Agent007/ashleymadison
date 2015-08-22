CREATE DATABASE IF NOT EXISTS ashleymadison;
USE ashleymadison;
DROP TABLE IF EXISTS rawcreditcardtransactions;
CREATE EXTERNAL TABLE rawcreditcardtransactions (
account STRING,
account_name STRING,
amount STRING,
auth_code STRING,
avs STRING,
brand STRING,
card_ending STRING,
cvd STRING,
first_name STRING,
last_name STRING,
merchant_trans_id STRING,
option_code STRING,
day STRING, -- "date" cannot be the column name since it's a keyword
txn_id STRING,
conf_no STRING,
error_code STRING,
auth_type STRING,
type STRING,
txt_city STRING,
txt_country STRING,
txt_email STRING,
txt_phone STRING,
txt_state STRING,
txt_addr1 STRING,
txt_addr2 STRING,
zip STRING,
consumer_ip STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/ashleymadison/creditcardtransactions';

-- Create clean data...
DROP TABLE IF EXISTS creditcardtransactions;
CREATE TABLE creditcardtransactions
STORED AS PARQUET
AS
SELECT
cast(regexp_extract(account, '"(.*)"', 1) AS BIGINT) AS account,
regexp_extract(account_name, '"(.*)"', 1) AS account_name,
cast(regexp_extract(amount, '"(.*)"', 1) AS DECIMAL(38,2)) AS amount,
regexp_extract(auth_code, '"(.*)"', 1) AS auth_code,
regexp_extract(avs, '"(.*)"', 1) AS avs,
regexp_extract(brand, '"(.*)"', 1) AS brand,
cast(regexp_extract(card_ending, '"(.*)"', 1) AS SMALLINT) AS card_ending,
regexp_extract(cvd, '"(.*)"', 1) AS cvd,
regexp_extract(first_name, '"(.*)"', 1) AS unknown,
regexp_extract(last_name, '"(.*)"', 1) AS name, -- The whole name seems to be in the "last name" column
cast(regexp_extract(merchant_trans_id, '"(.*)"', 1) AS BIGINT) AS merchant_trans_id,
regexp_extract(option_code, '"(.*)"', 1) AS option_code,
cast(regexp_extract(day, '"(.*)"', 1) AS TIMESTAMP) AS dateandtime,
cast(regexp_extract(txn_id, '"(.*)"', 1) AS BIGINT) AS txn_id,
cast(regexp_extract(conf_no, '"(.*)"', 1) AS BIGINT) AS conf_no,
cast(regexp_extract(error_code, '"(.*)"', 1) AS BIGINT) AS error_code,
regexp_extract(auth_type, '"(.*)"', 1) AS auth_type,
regexp_extract(type, '"(.*)"', 1) AS type,
regexp_extract(txt_city, '"(.*)"', 1) AS city,
regexp_extract(txt_country, '"(.*)"', 1) AS country,
regexp_extract(txt_email, '"(.*)"', 1) AS email,
regexp_extract(txt_phone, '"(.*)"', 1) AS phone,
regexp_extract(txt_state, '"(.*)"', 1) AS state,
regexp_extract(txt_addr1, '"(.*)"', 1) AS address1,
regexp_extract(txt_addr2, '"(.*)"', 1) AS address2,
regexp_extract(zip, '"(.*)"', 1) AS zip,
regexp_extract(consumer_ip, '"(.*)"', 1) AS ip_address
FROM rawcreditcardtransactions WHERE account <> '' AND account IS NOT NULL AND account <> 'ACCOUNT';
