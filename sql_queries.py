import configparser
import psycopg2

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

staging_events_table_create= """
CREATE TABLE IF NOT EXISTS staging_events (

    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    iteminsession INTEGER PRIMARY KEY,
    lastname VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionid INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    useragent VARCHAR,
    userid FLOAT
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id TEXT PRIMARY KEY,
    title VARCHAR(1024),
    duration FLOAT,
    year FLOAT,
    num_songs FLOAT,
    artist_id TEXT,
    artist_name VARCHAR(1024),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(1024)
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time INTEGER NOT NULL REFERENCES time(start_time) sortkey,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL REFERENCES songs(song_id) distkey,
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
    session_id INTEGER NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER NOT NULL PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    level VARCHAR NOT NULL
) DISTSTYLE all;
"""


song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id) sortkey distkey,
    year INTEGER, 
    duration FLOAT
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
) DISTSTYLE all;
"""


time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time INTEGER NOT NULL PRIMARY KEY, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
) DISTSTYLE all;
"""




# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()




# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


