--\c project;

-- Optional
START TRANSACTION;

create table games( 
	app_id integer not null primary key,
	title varchar(256),
	date_release date,
	win boolean,
	mac boolean,
	linux boolean,
	rating varchar(32),
	positive_ratio integer,
	user_reviews integer,
	price_final real,
	price_original real,
	discount real,
	steam_deck boolean
);

create table users(
	user_id integer not null primary key,
	products integer,
	reviews integer
);

create table recommendations(
	app_id integer not null,
	helpful integer,
	funny integer,
	date date,
	is_recommended boolean,
	hours real,
	user_id integer not null,
	review_id integer not null primary key
);

SET datestyle TO iso, ymd;

\COPY games from 'data/games.csv' delimiter ',' CSV header null as 'null';
\COPY users from 'data/users.csv' delimiter ',' CSV header null as 'null';
\COPY recommendations from 'data/recommendations.csv' delimiter ',' CSV header null as 'null';

commit;

-- Add constraints
-- FKs
alter table recommendations add constraint fk_rec_games_app_id foreign key(app_id) references games(app_id);

alter table recommendations add constraint fk_rec_users_user_id foreign key(user_id) references users(user_id);

select * from users limit 5;
select * from games limit 5;
select * from recommendations limit 5;



