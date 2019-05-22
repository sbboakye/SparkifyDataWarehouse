import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
REGION = config.get('DEFAULT', 'REGION')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE "staging_events" (
        "id" int IDENTITY(1,1) NOT NULL,
        "artist" varchar,
        "auth" varchar,
        "firstName" varchar,
        "gender" varchar(1),
        "itemSession" int,
        "lastName" varchar,
        "length" double precision,
        "level" varchar,
        "location" varchar,
        "method" varchar,
        "page" varchar,
        "registration" bigint,
        "sessionId" int,
        "song" varchar,
        "status" int,
        "ts" varchar,
        "userAgent" varchar,
        "userId" int
    ) DISTKEY("song") SORTKEY("ts");;
""")

staging_songs_table_create = ("""
    CREATE TABLE "staging_songs" (
        "id" int IDENTITY(1,1) NOT NULL,
        "num_songs" int,
        "artist_id" varchar,
        "artist_latitude" double precision,
        "artist_longitude" double precision,
        "artist_location" varchar,
        "artist_name" varchar,
        "song_id" varchar,
        "title" varchar,
        "duration" double precision,
        "year" int
    ) DISTKEY("title");
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        "songplay_id" int IDENTITY(1,1) NOT NULL,
        "start_time" timestamp,
        "user_id" int,
        "level" varchar,
        "song_id" varchar,
        "artist_id" varchar,
        "session_id" int,
        "location" varchar,
        "user_agent" varchar
    ) DISTKEY("song_id") SORTKEY("start_time");
""")

user_table_create = ("""
    CREATE TABLE users (
        "user_id" int NOT NULL PRIMARY KEY,
        "first_name" varchar,
        "last_name" varchar,
        "gender" varchar,
        "level" varchar
    ) SORTKEY("user_id");
""")

song_table_create = ("""
    CREATE TABLE songs (
        "song_id" character varying(20) NOT NULL PRIMARY KEY,
        "title" varchar,
        "artist_id" varchar,
        "year" int,
        "duration" double precision
    ) DISTKEY("song_id") SORTKEY("song_id");
""")

artist_table_create = ("""
    CREATE TABLE artists (
        "artist_id" character varying(20) NOT NULL PRIMARY KEY,
        "name" varchar,
        "location" varchar,
        "latitude" double precision,
        "longitude" double precision
    ) SORTKEY("artist_id");
""")

time_table_create = ("""
    CREATE TABLE time (
        "start_time" timestamp NOT NULL PRIMARY KEY,
        "hour" int,
        "day" int,
        "week" int,
        "month" int,
        "year" int,
        "weekday" int
    ) SORTKEY("start_time");
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events FROM '{LOG_DATA}'
    CREDENTIALS 'aws_iam_role={ARN}'
    region 'us-west-2'
    FORMAT AS JSON '{LOG_JSONPATH}';
""")

staging_songs_copy = (f"""
    COPY staging_songs FROM '{SONG_DATA}'
    CREDENTIALS 'aws_iam_role={ARN}'
    region 'us-west-2'
    FORMAT AS JSON 'auto';
""")

# FINAL TABLES
# timestamp 'epoch' + your_timestamp_column * interval '1 second'
# CAST(your_timestamp_column AS BIGINT)/1000
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id,
                    artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second', e.userId, e.level, s.song_id, s.artist_id,
            e.sessionId, e.location, e.userAgent
        FROM staging_events e
        LEFT JOIN staging_songs s
            ON e.artist = s.artist_name
            AND e.song = s.title
        WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM staging_events
        WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time,
           EXTRACT(hour FROM start_time),
           EXTRACT(day FROM start_time),
           EXTRACT(week FROM start_time),
           EXTRACT(month FROM start_time),
           EXTRACT(year FROM start_time),
           EXTRACT(weekday FROM start_time)
        FROM songplays
        WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
