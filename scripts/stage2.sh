#!/bin/bash

hdfs dfs -put /project/avsc/*.avsc /project/avsc

hive -f sql/db.hql > /data/hive_results.txt
