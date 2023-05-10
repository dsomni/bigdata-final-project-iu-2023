USE projectdb;

WITH win AS (
  SELECT COUNT(*) AS n FROM games_part AS games
  WHERE games.win = true
),
mac AS  (
  SELECT COUNT(*) AS n FROM games_part AS games
  WHERE games.mac = true
),
linux AS  (
  SELECT COUNT(*) AS n FROM games_part AS games
  WHERE games.linux = true
),
steam_deck AS  (
  SELECT COUNT(*) AS n FROM games_part AS games
  WHERE games.steam_deck = true
),
total AS (
  SELECT COUNT (*) AS n FROM games_part AS games
)
INSERT OVERWRITE LOCAL DIRECTORY '/root/q1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT ROUND(CAST(CAST(win.n AS FLOAT)/total.n*100 AS DECIMAL), 2) AS win_percent,
ROUND(CAST(CAST(mac.n AS FLOAT)/total.n*100 AS DECIMAL), 2) AS mac_percent,
ROUND(CAST(CAST(linux.n AS FLOAT)/total.n*100 AS DECIMAL), 2) AS linux_percent,
ROUND(CAST(CAST(steam_deck.n AS FLOAT)/total.n*100 AS DECIMAL), 2) AS steam_deck_percent FROM win, mac, linux, steam_deck, total;


WITH recommended_apps AS (
  SELECT DISTINCT app_id FROM recommendations_part WHERE is_recommended = true
)
INSERT OVERWRITE LOCAL DIRECTORY '/root/q2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT COUNT(*) as recommendations_of_non_win FROM recommended_apps AS r, games_part AS g WHERE r.app_id = g.app_id AND g.win = FALSE;


INSERT OVERWRITE LOCAL DIRECTORY '/root/q3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT ROUND( CAST( CORR(products, reviews )AS DECIMAL), 2) AS products_reviews_corr FROM users_part;

INSERT OVERWRITE LOCAL DIRECTORY '/root/q4'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT ROUND(CAST(CAST(rec AS FLOAT)*100/total AS DECIMAL), 2) AS recommendations_percent FROM (SELECT COUNT(*) AS rec FROM recommendations_part WHERE is_recommended = true) AS recom, (SELECT COUNT(*) AS total FROM recommendations_part) AS overall;

WITH game_reviews AS (
  SELECT app_id, COUNT(*) AS reviews FROM recommendations_part GROUP BY app_id
)
INSERT OVERWRITE LOCAL DIRECTORY '/root/q5'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *, ROUND(CAST((reviews_number/games_number) AS DECIMAL), 3) AS avg_reviews_per_game
FROM (
SELECT year(date_release) AS published_year,
COUNT(*) AS games_number,
COALESCE(SUM(game_reviews.reviews), 0) AS reviews_number,
ROUND(CAST(AVG(price_original) AS DECIMAL), 3) AS avg_price_original,
ROUND(CAST(AVG(price_final) AS DECIMAL), 3) AS avg_price_final
FROM games_part LEFT JOIN game_reviews ON games_part.app_id = game_reviews.app_id
GROUP BY year(date_release)
ORDER BY reviews_number DESC) AS temp;

INSERT OVERWRITE LOCAL DIRECTORY '/root/q6'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT title, date_release, rating, positive_ratio, user_reviews 
FROM games_part
WHERE year(date_release) = 1998;
