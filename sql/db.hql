
DROP DATABASE IF EXISTS projectdb CASCADE;

CREATE DATABASE projectdb;
USE projectdb;

SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;

CREATE EXTERNAL TABLE games STORED AS AVRO LOCATION '/project/games' TBLPROPERTIES ('avro.schema.url'='/project/avsc/games.avsc');
CREATE EXTERNAL TABLE users STORED AS AVRO LOCATION '/project/users' TBLPROPERTIES ('avro.schema.url'='/project/avsc/users.avsc');
CREATE EXTERNAL TABLE recommendations STORED AS AVRO LOCATION '/project/recommendations' TBLPROPERTIES ('avro.schema.url'='/project/avsc/recommendations.avsc');


SELECT count(*) FROM games;
SELECT count(*) FROM recommendations;
SELECT count(*) FROM users;






