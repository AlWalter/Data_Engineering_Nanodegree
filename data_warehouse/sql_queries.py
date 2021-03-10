import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    first_name      VARCHAR,
    gender          VARCHAR,
    item_in_session INTEGER,
    last_name       VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    VARCHAR,
    session_id      INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              VARCHAR,
    user_agent      VARCHAR,
    user_id         INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     VARCHAR,
    artist_longitude    VARCHAR,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id         BIGINT      IDENTITY(0,1) PRIMARY KEY SORTKEY,
    start_time          VARCHAR     NOT NULL, 
    user_id             INTEGER     NOT NULL, 
    level               VARCHAR, 
    song_id             VARCHAR     NOT NULL, 
    artist_id           VARCHAR     NOT NULL DISTKEY, 
    session_id          INTEGER, 
    location            VARCHAR, 
    user_agent          VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER NOT NULL, 
    first_name  VARCHAR, 
    last_name   VARCHAR, 
    gender      VARCHAR, 
    level       VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR     PRIMARY KEY, 
    title       VARCHAR, 
    artist_id   VARCHAR     DISTKEY, 
    year        INTEGER, 
    duration    FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id   VARCHAR     PRIMARY KEY DISTKEY,
    name        VARCHAR, 
    location    VARCHAR, 
    latitude    VARCHAR, 
    longitude   VARCHAR
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP   PRIMARY KEY,
    hour        INTEGER,
    day         INTEGER,
    week        INTEGER,
    month       INTEGER,
    year        INTEGER,
    weekday     INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
) 
SELECT 
    TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent  
FROM staging_events e 
JOIN staging_songs s ON e.song = s.title;
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
) 
SELECT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM staging_events
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
) 
SELECT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
) 
SELECT 
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as  longitude
FROM staging_songs
WHERE artist_id IS NOT NULL; 
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
) 
SELECT DISTINCT 
    TIMESTAMP 'epoch' + (ts::bigint)/1000 * INTERVAL '1 second' as start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) as month,
    EXTRACT(year FROM start_time) as year,
    EXTRACT (weekday FROM start_time) as weekday
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
