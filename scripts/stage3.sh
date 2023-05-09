#!/bin/bash

hdfs dfs -rm -r /project/pda

spark-submit --jars /usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,/usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar --packages org.apache.spark:spark-avro_2.12:3.0.3 scripts/model.py


hdfs dfs -get /project/pda/* /root/pda/


cat /root/pda/als_params/* >> output/als_params.csv
cat /root/pda/als_scores/* >> output/als_scores.csv
cat /root/pda/rf_params/* >> output/rf_params.csv
cat /root/pda/rf_scores/* >> output/rf_scores.csv
cat /root/pda/cv_models/* >> output/cv_models.csv
cat /root/pda/best_model_scores/* >> output/best_model_scores.csv
cat /root/pda/recommendations/* >> output/recommendations.json

