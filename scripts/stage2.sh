#!/bin/bash

hdfs dfs -rm -r /project/projectdata

hdfs dfs -put -f /project/avsc/*.avsc /project/avsc

hive -f sql/db.hql > ./data/hive_results.txt

hive -f sql/partition.hql > ./data/hive_partition_results.txt

hive -f sql/eda.hql 

echo "win_percent, mac_percent, linux_percent, steam_deck_percent" > output/q1.csv
cat /root/q1/* >> output/q1.csv
cat output/q1.csv

echo "recommendations_of_non_win" > output/q2.csv
cat /root/q2/* >> output/q2.csv
cat output/q2.csv

echo "products_reviews_corr" > output/q3.csv
cat /root/q3/* >> output/q3.csv
cat output/q3.csv

echo "recommendation_percent" > output/q4.csv
cat /root/q4/* >> output/q4.csv
cat output/q4.csv

echo "published_year, games_number, reviews_number, avg_price_original, avg_price_final, avg_reviews_per_game" > output/q5.csv
cat /root/q5/* >> output/q5.csv
cat output/q5.csv

echo "title, date_release, rating, positive_ratio, user_reviews" > output/q6.csv
cat /root/q6/* >> output/q6.csv
cat output/q6.csv
