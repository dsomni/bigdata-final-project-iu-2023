#!/bin/bash

hdfs dfs -rm -r /project/pda
hdfs dfs -rm -r /project/models

spark-submit --jars /usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,/usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar --packages org.apache.spark:spark-avro_2.12:3.0.3 scripts/model.py

rm -r /root/pda
mkdir /root/pda

hdfs dfs -get /project/models/* models/
hdfs dfs -get /project/pda/* /root/pda/

cat /root/pda/cv_als_config/* >> output/cv_als_config.csv
cat /root/pda/best_als_params/* >> output/best_als_params.csv
cat /root/pda/best_als_scores/* >> output/best_als_scores.csv
cat /root/pda/als_recommendations/* >> output/als_recommendations.json

cat /root/pda/cv_rf_config/* >> output/cv_rf_config.csv
cat /root/pda/rf_features/* >> output/rf_features.csv
cat /root/pda/best_rf_params/* >> output/best_rf_params.csv
cat /root/pda/best_rf_scores/* >> output/best_rf_scores.csv
cat /root/pda/rf_recommendations/* >> output/rf_recommendations.json

