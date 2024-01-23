from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    "jm_demo",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    resume_cluster = RedshiftResumeClusterOperator(
        task_id='resume_cluster',
        cluster_identifier="jm-portfolio-airflow-cluster",
    )

    wait_cluster_available = RedshiftClusterSensor(
        task_id="wait_cluster_available",
        cluster_identifier="jm-portfolio-airflow-cluster",
        target_status="available",
        poke_interval=15,
        timeout=60 * 30,
    )
    
    create_events_staging = RedshiftDataOperator(
        task_id="create_events_staging",
        region="us-east-1",
        cluster_identifier="jm-portfolio-airflow-cluster",
        database="dev",
        sql="""
            CREATE TABLE IF NOT EXISTS public.staging_events (
                artist varchar(256),
                auth varchar(256),
                firstname varchar(256),
                gender varchar(256),
                iteminsession int4,
                lastname varchar(256),
                length numeric(18,0),
                "level" varchar(256),
                location varchar(256),
                "method" varchar(256),
                page varchar(256),
                registration numeric(18,0),
                sessionid int4,
                song varchar(256),
                status int4,
                ts int8,
                useragent varchar(256),
                userid int4
            );"""
    )

    create_songs_staging = RedshiftDataOperator(
        task_id="create_songs_staging",
        region="us-east-1",
        cluster_identifier="jm-portfolio-airflow-cluster",
        database="dev",
        sql="""
            CREATE TABLE IF NOT EXISTS public.staging_songs (
                num_songs int4,
                artist_id varchar(256),
                artist_name varchar(256),
                artist_latitude numeric(18,0),
                artist_longitude numeric(18,0),
                artist_location varchar(256),
                song_id varchar(256),
                title varchar(256),
                duration numeric(18,0),
                "year" int4
            );"""
    )

    create_songplays = RedshiftDataOperator(
        task_id="create_songplays",
        region="us-east-1",
        cluster_identifier="jm-portfolio-airflow-cluster",
        database="dev",
        sql="""
            CREATE TABLE IF NOT EXISTS public.fact_songplays (
                playid varchar(32) NOT NULL,
                start_time timestamp NOT NULL,
                userid int4 NOT NULL,
                "level" varchar(256),
                songid varchar(256),
                artistid varchar(256),
                sessionid int4,
                location varchar(256),
                user_agent varchar(256),
                CONSTRAINT songplays_pkey PRIMARY KEY (playid)
            );"""
    )

    create_dim_songs = RedshiftDataOperator(
        task_id="create_dim_songs",
        region="us-east-1",
        cluster_identifier="jm-portfolio-airflow-cluster",
        database="dev",
        sql="""
            CREATE TABLE IF NOT EXISTS public.dim_songs (
                songid varchar(256) NOT NULL,
                title varchar(256),
                artistid varchar(256),
                "year" int4,
                duration numeric(18,0),
                CONSTRAINT songs_pkey PRIMARY KEY (songid)
            );"""
    )

    create_dim_users = RedshiftDataOperator(
        task_id="create_dim_users",
        region="us-east-1",
        cluster_identifier="jm-portfolio-airflow-cluster",
        database="dev",
        sql="""
            CREATE TABLE IF NOT EXISTS public.dim_users (
                userid int4 NOT NULL,
                first_name varchar(256),
                last_name varchar(256),
                gender varchar(256),
                "level" varchar(256),
                CONSTRAINT users_pkey PRIMARY KEY (userid)
            );"""
    )

    create_artists = RedshiftDataOperator(
        task_id="create_artists",
        region="us-east-1",
        cluster_identifier="jm-portfolio-airflow-cluster",
        database="dev",
        sql="""
            CREATE TABLE IF NOT EXISTS public.dim_artists (
                artistid varchar(256) NOT NULL,
                name varchar(256),
                location varchar(256),
                lattitude numeric(18,0),
                longitude numeric(18,0)
            );"""
    )

    create_time = RedshiftDataOperator(
        task_id="create_time",
        region="us-east-1",
        cluster_identifier="jm-portfolio-airflow-cluster",
        database="dev",
        sql="""
            CREATE TABLE IF NOT EXISTS public.dim_time (
                start_time timestamp NOT NULL,
                "hour" int4,
                "day" int4,
                week int4,
                "month" varchar(256),
                "year" int4,
                weekday varchar(256),
                CONSTRAINT time_pkey PRIMARY KEY (start_time)
            );"""
    )

    begin_loading_data = EmptyOperator(task_id="begin_loading_data")

    stage_events_to_redshift = S3ToRedshiftOperator(
        task_id="events_to_staging",
        s3_bucket="jm-portfolio-data",
        s3_key="airflow/log_data/",
        schema="public",
        table="staging_events",
        copy_options=[
            "json 'auto'"
        ]
    )

    stage_songs_to_redshift = S3ToRedshiftOperator(
        task_id="songs_to_staging",
        s3_bucket="jm-portfolio-data",
        s3_key="airflow/song_data/",
        schema="public",
        table="staging_songs",
        copy_options=[
            "json 'auto'"
        ]
    )

    load_songplays = RedshiftDataOperator(
        task_id="load_songplays",
        database="dev",
        cluster_identifier="jm-portfolio-airflow-cluster",
        sql="""
            INSERT INTO fact_songplays
            SELECT
                md5(events.sessionid || events.ts) playid,
                (TIMESTAMP 'epoch' + events.ts / 1000 * INTERVAL '1 Second ') start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
            FROM staging_events events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
            WHERE page='NextSong';"""
    )

    load_songs = RedshiftDataOperator(
        task_id="load_songs",
        database="dev",
        cluster_identifier="jm-portfolio-airflow-cluster",
        sql="""
            INSERT INTO dim_songs
            SELECT 
                distinct song_id, 
                title, 
                artist_id, 
                year, 
                duration
            FROM staging_songs"""
    )

    load_users = RedshiftDataOperator(
        task_id="load_users",
        database="dev",
        cluster_identifier="jm-portfolio-airflow-cluster",
        sql="""
            INSERT INTO dim_users
            SELECT 
                distinct userid, 
                firstname, 
                lastname, 
                gender, 
                level
            FROM staging_events
            WHERE page='NextSong';"""
    )

    load_time = RedshiftDataOperator(
        task_id="load_time",
        database="dev",
        cluster_identifier="jm-portfolio-airflow-cluster",
        sql="""
            INSERT INTO dim_time
            SELECT 
                start_time, 
                extract(hour from start_time), 
                extract(day from start_time), 
                extract(week from start_time), 
                extract(month from start_time), 
                extract(year from start_time), 
                extract(dayofweek from start_time)
            FROM fact_songplays;"""
    )

    load_artists = RedshiftDataOperator(
        task_id="load_artists",
        database="dev",
        cluster_identifier="jm-portfolio-airflow-cluster",
        sql="""
            INSERT INTO dim_artists
            SELECT 
                distinct artist_id, 
                artist_name, 
                artist_location, 
                artist_latitude, 
                artist_longitude
            FROM staging_songs;"""
    )

    resume_cluster >> wait_cluster_available
    wait_cluster_available >> [create_events_staging, create_songs_staging, create_songplays, create_dim_songs, create_dim_users, create_artists, create_time] >> begin_loading_data
    begin_loading_data >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays
    load_songplays >> [load_songs, load_users, load_time, load_artists]
    