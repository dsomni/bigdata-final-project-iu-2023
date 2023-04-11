SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

SET hive.enforce.bucketing=true;

use projectdb;

drop table if exists games_part;
drop table if exists users_part;
drop table if exists recommendations_part;

create external table games_part(
        app_id int,
        title varchar(256),
        date_release date,
        win boolean,
        positive_ratio int,
        user_reviews int,
        price_final float,
        price_original float,
        discount float,
        steam_deck boolean
) 	partitioned by (rating varchar(32), linux boolean, mac boolean)
	clustered by (app_id) into 10 buckets
	stored as avro location '/projectdata'
	tblproperties ('AVRO.COMPRESS'='SNAPPY');

create external table users_part(
        user_id int,
        products int,
        reviews int
) 	clustered by (user_id) into 50 buckets
	stored as avro location '/projectdata'
        tblproperties ('AVRO.COMPRESS'='SNAPPY');

create external table recommendations_part(
        app_id int,
        helpful int,
        funny int,
        date_review date,
        hours float,
        user_id int,
        review_id int
) 	partitioned by (is_recommended boolean)
	clustered by (review_id) into 100 buckets
	stored as avro location '/projectdata'
        tblproperties ('AVRO.COMPRESS'='SNAPPY');


insert into games_part partition (rating, linux, mac) SELECT app_id, title, cast(to_date(from_utc_timestamp(date_release, "+00")) as date) as date_release, win, positive_ratio, user_reviews, price_final, price_original, discount, steam_deck, rating, linux, mac FROM games;
insert into users_part SELECT * FROM users limit 5;
insert into recommendations_part partition (is_recommended) SELECT app_id, helpful, funny,  cast(to_date(from_utc_timestamp(`date`, "+00")) as date) as date_review, hours, user_id, review_id, is_recommended  FROM recommendations;
