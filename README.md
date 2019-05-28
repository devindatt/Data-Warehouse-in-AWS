# Project: Data Warehouse on AWS

[![N|Solid](https://www.wellscore.com/wp-content/uploads/2016/04/data-warehousing.png)

![Build Status](https://www.gartner.com/imagesrv/peer-insights/icon-mrkt-data-warehouse@2x.png)


## Introduction

Our task as a Data Engineer for fictitious music company Sparkify is to design and code an ETL pipeline that extracts music data from an AWS S3 bucket, stages them in a Redshift cluster, and transform that data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. We'll test our database and ETL pipeline by running queries given by the Sparkify analytics team and compare your results with their expected results.

##### Steps To Complete:
1) Create dimensional, facts, and staging tables
2) Load data from the S3 bucket into staging tables on Redshift
3) Execute SQL statements that create the analytics tables

###### Files & Links Used:

| File | Description |
| ------ | ------ |
| song_data | Song JSON data in S3 bucket 'udacity-dend'|
| log_data | Log JSON data in S3 bucket 'udacity-dend'|
| dwh.cfg | Configuration file for AWS resource access |
| sql_queries.py | Python script create, copy and insert SQL tables  |
| create_tables.py | Python script to create AWS resources and SQL tables|
| etl.py | Python script to load and insert SQL staging, dimension and facts tables  |

### Amazon Web Services

To create the cluster with your credentials place your AWS key and secret into dwh.cfg in the following structure:

AWS_KEY=<your aws key>
AWS_SECRET=<your aws secret>

Note, please do not upload these credentials to GitHub if you plan to share this file.

### Running the ETL Process

Running the ETL process is divided in three parts:
1. Creating the AWS resources
2. Creating the Postgres tables
3. Populating the tables

##### 1) Creating the AWS resources
- Import boto3 package
- Create the EC2, S3
- Access the IAM user and Redshift cluster
- Create IAM role and attach the policy
- Add parameters to the AWS resources
- Update the VPC security group for access

##### 2) Creating the Postgres tables
- Import configparser & psycopg2 packages
- Drop any existing tables in database
- Recreate all new tables in database

For our data warehouse, one fact table, four dimension tables and two staging tables need to be created.

##### Fact Table: songplays
- Used 'songplay_id' as primary key and used 'start_time' to sort and 'song_id' to distribute the rows over the nodes.
```songplay_table_create = 
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP sortkey,
    user_id INTEGER,
    level VARCHAR,
    song_id VARCHAR  distkey,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
```

##### Dimension Table: Users

- Used 'user_id' as primary key and since small we distribute this table on all nodes.
```
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER NOT NULL PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
) DISTSTYLE all;
```

##### Dimension Table: Songs
- Used 'song_id' as primary key and used 'artist_id to sort and distribute the rows over the nodes.
```
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL sortkey distkey,
    year INTEGER, 
    duration FLOAT
```
##### Dimension Table: Artists
- Used 'artist_id' as primary key and since small we distribute this table on all nodes.
```
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
) DISTSTYLE all;
```
##### Dimension Table: Time
- Used 'start_time' as primary key and since small we distribute this table on all nodes.
```
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY, 
    hour INTEGER, 
    day INTEGER,
    week INTEGER,
    month VARCHAR,
    year INTEGER,
    weekday VARCHAR 
) DISTSTYLE all;
```
##### Staging Table: Staging_events
```
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
    session_id INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    useragent VARCHAR,
    user_id INTEGER
);
```

##### Staging Table: Staging_songs

```
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id TEXT PRIMARY KEY,
    title VARCHAR(1024),
    duration FLOAT,
    year INTEGER,
    num_songs INTEGER,
    artist_id TEXT,
    artist_name VARCHAR(1024),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(1024)
);
```

##### 3) Populating the tables
- Load the 2 JSON files from AWS S3 bucket
```
 copy staging_events
    from {}
    iam_role {}
    json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])
```
```
copy staging_songs
    from {}
    iam_role {}
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])
```

- Insert data into Facts & Dimensional tables in Redshift cluster

##### Insert into Fact: Songplay
- Needed to convert 'ts' into a proper timestamp
- join both songs and artists tables, and  staging_events tables and left join with staging_songs tables
```
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT events.start_time, events.user_id, events.level, songs.song_id, songs.artist_id, events.session_id, events.location, events.useragent
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
          FROM staging_events
          WHERE page='NextSong') events
    LEFT JOIN staging_songs songs
    ON events.song = songs.title
    AND events.artist = songs.artist_name
    AND events.length = songs.duration
```

##### Insert into Dimensional: User
```
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT distinct user_id, firstname, lastname, gender, level
    FROM staging_events
    WHERE page='NextSong'
```


##### Insert into Dimensional: Songs
```
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT distinct song_id, title, artist_id, year, duration
    FROM staging_songs
```

##### Insert into Dimensional: Artists
```
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
```

##### Insert into Dimensional: Time
```
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT distinct start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
           extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
    FROM songplays
```
## Useful Analysis Samples

##### Bar graph by Music Agent

![Bar graph by Music Agent](https://github.com/devindatt/Data-Warehouse-in-AWS/blob/master/bar_graph_songplay_by_agent.png)

##### Bar graph by Location

![Bar graph by Music Agent](https://github.com/devindatt/Data-Warehouse-in-AWS/blob/master/bar_graph_songplay_by_location.png)

##### Bar graph by Song-ID

![Bar graph by Music Agent](https://github.com/devindatt/Data-Warehouse-in-AWS/blob/master/bar_graph_songplay_by_song_id.png)

##### Bar graph by Time of Play

![Bar graph by Music Agent](https://github.com/devindatt/Data-Warehouse-in-AWS/blob/master/bar_graph_songplay_by_time.png)


## Resources
- The Song dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/)
- The Log dataset are files in JSON format generated by [Event Simulator](https://github.com/Interana/eventsim)
- Using ['timestamp'](https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift)
 
