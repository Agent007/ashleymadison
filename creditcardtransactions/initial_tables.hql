CREATE DATABASE IF NOT EXISTS ashleymadison;
USE ashleymadison;

DROP TABLE IF EXISTS creditcardtransactions_json;
CREATE EXTERNAL TABLE creditcardtransactions_json ( json string ) LOCATION '/user/ashleymadison/creditcardtransactions/D_json/';

DROP TABLE IF EXISTS creditcardtransactions_strings;
CREATE TABLE creditcardtransactions_strings
STORED AS PARQUET
AS
SELECT
get_json_object(json, '$.account') AS account,
get_json_object(json, '$.account_name') AS account_name,
get_json_object(json, '$.amount') AS amount,
get_json_object(json, '$.auth_code') AS auth_code,
get_json_object(json, '$.avs') AS avs,
get_json_object(json, '$.brand') AS brand,
get_json_object(json, '$.card_ending') AS card_ending,
get_json_object(json, '$.cvd') AS cvd,
get_json_object(json, '$.unknown') AS unknown,
get_json_object(json, '$.name') AS name,
get_json_object(json, '$.merchant_trans_id') AS merchant_trans_id,
get_json_object(json, '$.option_code') AS option_code,
get_json_object(json, '$.dateandtime') AS dateandtime,
get_json_object(json, '$.txn_id') AS txn_id,
get_json_object(json, '$.conf_no') AS conf_no,
get_json_object(json, '$.error_code') AS error_code,
get_json_object(json, '$.auth_type') AS auth_type,
get_json_object(json, '$.type') AS type,
get_json_object(json, '$.city') AS city,
get_json_object(json, '$.country') AS country,
get_json_object(json, '$.email') AS email,
get_json_object(json, '$.phone') AS phone,
get_json_object(json, '$.state') AS state,
get_json_object(json, '$.address1') AS address1,
get_json_object(json, '$.address2') AS address2,
get_json_object(json, '$.zip') AS zip,
get_json_object(json, '$.ip_address') AS ip_address
FROM creditcardtransactions_json;
