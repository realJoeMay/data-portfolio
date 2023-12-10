import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS events_staging (
    artist        varchar(512),
    auth          varchar,
    firstName     varchar,
    gender        char(1),
    itemInSession integer,
    lastName      varchar,
    length        decimal,
    level         varchar,
    location      varchar,
    method        varchar,
    page          varchar,
    registration  decimal,
    sessionID     integer,
    song          varchar,
    status        integer,
    ts            bigint,
    userAgent     varchar,
    userId        integer
);""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_staging (
    song_id          varchar,
    artist_id        varchar,
    artist_location  varchar(512),
    artist_latitude  decimal,
    artist_longitude decimal,
    artist_name      varchar(512),
    duration         decimal,
    num_songs        integer,
    title            varchar(512),
    year             integer
);""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id bigint PRIMARY KEY IDENTITY(0, 1),
    start_time  timestamp,
    user_id     integer,
    level       varchar,
    song_id     varchar,
    artist_id   varchar,
    session_id  integer,
    location    varchar,
    user_agent  varchar
);""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id    integer PRIMARY KEY,
    first_name varchar,
    last_name  varchar,
    gender     char(1),
    level      varchar
);""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id   varchar PRIMARY KEY,
    title     varchar(512),
    artist_id varchar,
    year      integer,
    duration  decimal
);""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name      varchar(512),
    location  varchar(512),
    lattitude decimal,
    longitude decimal
);""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour       integer,
    day        integer,
    week       integer,
    month      integer,
    year       integer,
    weekday    integer
);""")

# STAGING TABLES

staging_events_copy = (f"""
COPY events_staging 
FROM '{config.get("S3", "LOG_DATA")}'
CREDENTIALS 'aws_iam_role={config.get("IAM_ROLE", "ARN")}'
REGION 'us-west-2'
json 'auto ignorecase'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format()

staging_songs_copy = (f"""
COPY songs_staging 
FROM '{config.get("S3", "SONG_DATA")}'
CREDENTIALS 'aws_iam_role={config.get("IAM_ROLE", "ARN")}'
REGION 'us-west-2'
json 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format()

# FINAL TABLES

user_table_insert = ("""
INSERT INTO users
SELECT
    DISTINCT userid,
    firstname,
    lastname,
    gender,
    level
FROM events_staging
WHERE userid IS NOT NULL;
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
FROM songs_staging;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    lattitude,
    longitude
)
SELECT 
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM songs_staging;
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
SELECT
    DISTINCT TIMESTAMP 'epoch' + ts / 1000 * interval '1 second' AS t,
    EXTRACT(hour from t),
    EXTRACT(day from t),
    EXTRACT(week from t),
    EXTRACT(month from t),
    EXTRACT(year from t),
    EXTRACT(weekday from t)
FROM events_staging;
""")

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
    TIMESTAMP 'epoch' + e.ts / 1000 * interval '1 second' AS t,
    e.userid,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionid,
    e.location,
    e.useragent
FROM events_staging as e
LEFT JOIN songs_staging as s
ON e.song = s.title
WHERE page = 'NextSong';
""")

# QUERY LISTS

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]