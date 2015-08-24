#!/usr/bin/env bash

# TODO automatically check for, and clean, prematurely truncated daily transaction CSV files

# Merge CSV files
cat *.csv > merged.csv # Your actual file directory path may differ

# We need CSVInputFormat to properly extract multi-line fields wrapped in quotes. Spark CSV doesn't yet handle it.
# (https://github.com/databricks/spark-csv/issues/72)
# TODO properly recognize commas within quotes instead of mistaking them for field termination characters
git clone git@github.com:Agent007/CSVInputFormat.git
cd CSVInputFormat
git checkout Hadoop2.5.0
mvn package -Dmaven.test.skip=true
cd ..
spark-shell --master local[*] --jars CSVInputFormat/target/CSVInputFormat-1.0.jar  --executor-memory 16G --conf spark.storage.memoryFraction=0
