USE ashleymadison;

-- data quality checks for line breaks within text fields
SELECT address1 FROM creditcardtransactions_strings WHERE address1 LIKE '%\n%';
SELECT * FROM creditcardtransactions_json WHERE json LIKE '%HAUNG.JERRY590130@GMAIL.COM%';
-- after discovering the erroneous line found via the above query, I manually fixed the merged.csv and re-ran the Spark
-- Shell Scala script
