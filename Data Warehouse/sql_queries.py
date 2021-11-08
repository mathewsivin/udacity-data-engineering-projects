import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS"staging_events_table" (
    "artist" character varying(255),
    "auth" character varying(255),
    "firstName" character varying(255),
    "gender" character varying(10),
    "itemInSession" integer,
    "lastName" character varying(255),
    "length" double precision,
    "level" character varying(50),
    "location" character varying(255),
    "method" character varying(255),
    "page" character varying(255),
    "registration" character varying(100),
    "sessionId" integer,
    "song" character varying(255),
    "status" integer,
    "ts" character varying(255),
    "userAgent" character varying(255),
    "userId" integer
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS "staging_songs_table" (
    "artist_id" character varying(255),
    "artist_latitude" character varying(255),
    "artist_location" character varying(255),
    "artist_longitude" character varying(255),
    "artist_name" character varying(255),
    "duration" double precision,
    "num_songs" integer,
    "song_id" character varying(255),
    "title" character varying(255),
    "year" integer
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS "songplays" (
    "songplay_id" bigint identity not null sortkey,
    "start_time" timestamp not null,
    "user_id" character varying(255),
    "level" character varying(10) not null,
    "song_id" character varying(255) distkey not null,
    "artist_id" character varying(255) not null,
    "session_id" character varying(255) not null ,
    "location" character varying(255),
    "user_agent" character varying(255));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS "users" (
    "user_id" character varying(255) primary key,
    "first_name" character varying(255),
    "last_name" character varying(255),
    "gender" character varying(10),
    "level" character varying(50) not null) diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS "songs" (
    "song_id" character varying(255) sortkey distkey primary key not null ,
    "title" character varying(255) not null,
    "artist_id" character varying(255) not null,
    "year" integer not null,
    "duration" double precision not null);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS "artists" (
    "artist_id" character varying(255) sortkey distkey primary key not null,
    "name" character varying(255) not null,
    "location" character varying(255),
    "latitude" character varying(255),
    "longitude" character varying(255));
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS "time" (
    "start_time" timestamp sortkey primary key not null ,
    "hour" integer not null,
    "day" integer not null,
    "week" integer not null,
    "month" integer not null,
    "year" integer not null,
    "weekday" integer not null) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events_table from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}' 
    json 's3://udacity-dend/log_json_path.json' region 'us-west-2'
""").format(*config['IAM_ROLE'].values())

staging_songs_copy = ("""    copy staging_songs_table from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}' 
    json 'auto' region 'us-west-2'
""").format(*config['IAM_ROLE'].values())

# FINAL TABLES

songplay_table_insert = ("""insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select 
        cast(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as timestamp)
        ,userId
        ,level
        ,song_id
        ,artist_id
        ,sessionId
        ,location
        ,userAgent
from staging_events_table 
join staging_songs_table 
on  staging_songs_table.artist_name = staging_events_table.artist
and staging_songs_table.duration = staging_events_table.length 
and staging_songs_table.title = staging_events_table.song;
""")

user_table_insert = ("""insert into users (user_id, first_name, last_name, gender, level)
select distinct userid, firstname, lastname, gender, level from staging_events_table where page = 'NextSong';
""")

song_table_insert = ("""insert into songs (song_id, title, artist_id, year, duration)
select song_id, title, artist_id, year, duration from staging_songs_table;
""")

artist_table_insert = ("""insert into artists (artist_id, name, location, latitude, longitude)
select artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs_table;
""")

time_table_insert = ("""insert into time (start_time, hour, day, week, month, year, weekday)
select check_timestamp
       ,extract('hour' from  check_timestamp)
       ,extract('day' from  check_timestamp)
       ,extract('week' from  check_timestamp)
       ,extract('month' from  check_timestamp)
       ,extract('year' from  check_timestamp)
       ,extract('weekday' from  check_timestamp)
from (select cast(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as timestamp) as check_timestamp from staging_events_table) as base; 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
