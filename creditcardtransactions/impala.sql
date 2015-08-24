USE ashleymadison;

DROP TABLE IF EXISTS creditcardtransactions;
CREATE TABLE creditcardtransactions
STORED AS PARQUET
AS
SELECT
account,
account_name,
cast(amount AS DECIMAL(38,2)) AS amount,
auth_code,
avs,
brand,
card_ending,
cvd,
unknown,
name,
merchant_trans_id,
option_code,
cast(dateandtime AS TIMESTAMP) AS dateandtime,
txn_id,
conf_no,
error_code,
auth_type,
type,
city,
country,
email,
phone,
state,
address1,
address2,
zip,
ip_address
FROM creditcardtransactions_strings;
-- Check and compare counts
SELECT count(*) FROM creditcardtransactions_strings; -- 9685914
SELECT count(*) FROM creditcardtransactions; -- 9685914
SELECT sum(amount) FROM creditcardtransactions; -- 630403148.94 (Not all in US dollars)
-- $150M estimated revenue in 2014 according to BBC http://www.bbc.com/news/business-32746405
SELECT year(dateandtime) AS yr, sum(amount) AS yearly_revenue FROM creditcardtransactions GROUP BY yr ORDER BY yr;

-- data quality: check for nulls due to bad conversions from strings to numbers
SELECT count(*) FROM creditcardtransactions WHERE account IS NULL; -- 0
SELECT count(*) FROM creditcardtransactions_strings WHERE account IS NULL; -- 0
SELECT count(*) FROM creditcardtransactions_strings WHERE account = ''; -- 0

SELECT count(*) FROM creditcardtransactions WHERE amount IS NULL; -- 10640
SELECT count(*) FROM creditcardtransactions_strings WHERE amount IS NULL; -- 0
SELECT count(*) FROM creditcardtransactions_strings WHERE amount = ''; -- 0
SELECT * FROM creditcardtransactions_strings WHERE txn_id = '6985149'; -- There are negative amounts!

SELECT count(*) FROM creditcardtransactions WHERE card_ending IS NULL; --
SELECT count(*) FROM creditcardtransactions_strings WHERE card_ending IS NULL OR length(trim(card_ending)) = 0 OR upper(card_ending) LIKE '%NULL%'; -- 35370

SELECT count(*) FROM creditcardtransactions WHERE merchant_trans_id IS NULL; --
SELECT count(*) FROM creditcardtransactions_strings WHERE merchant_trans_id IS NULL OR length(trim(merchant_trans_id)) = 0 OR upper(merchant_trans_id) LIKE '%NULL%'; -- 7829
-- TODO invalid merchant_trans_id apparrently occurs for gift cards

SELECT count(*) FROM creditcardtransactions WHERE dateandtime IS NULL; -- 0

SELECT * FROM creditcardtransactions_strings WHERE dateandtime = '2015-05-16 01:35:30'; -- not all txn_id's are numbers


