#!/bin/bash

hdfs dfs -put /project/avsc/*.avsc /project/avsc

hive -f sql/db.hql > ./data/hive_results.txt

hive -f sql/partition.hql > ./data/hive_partition_results.txt

hive -f sql/eda.hql 

echo "win_percent, mac_percent, linux_percent, steam_deck_percent" > output/q1.csv
cat /root/q1/* >> output/q1.csv

echo "recommendations_of_non_win" > output/q2.csv
cat /root/q2/* >> output/q2.csv

echo "products_reviews_corr" > output/q3.csv
cat /root/q3/* >> output/q3.csv

echo "is_recommended, avg_hours" > output/q4.csv
cat /root/q4/* >> output/q4.csv

echo "published_year, games_number, reviews_number, avg_price_original, avg_price_final" > output/q5.csv
cat /root/q5/* >> output/q5.csv
