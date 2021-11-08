#./opt/airflow/start.sh

import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadDimensionOperator, LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries
from datetime import timedelta
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'udacity',
    'start_date': datetime.datetime.now(),
    #'end_date': datetime(2021, 2, 20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False
}
dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    #schedule_interval='@daily',
    max_active_runs=1)

    
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql")

stage_songs = StageToRedshiftOperator(
    task_id="stage_songs_id",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    #s3_key="song_data/A/A/A",
    table="staging_songs",
    json_type="auto"
)

stage_events = StageToRedshiftOperator(
    task_id="stage_events_id",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    #s3_key="log_data/2018/11",
    ##s3_key="log_data/{execution_date.year}/{execution_date.month}",
    table="staging_events",
    json_type = "s3://udacity-dend/log_json_path.json"
)


load_songplays_fact_table = LoadDimensionOperator(
    task_id="load_songplays_fact_table_id",
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="songplays",
    sql_statement=SqlQueries.songplay_table_insert,
    schema="public"
)


load_song_dim_table = LoadDimensionOperator(
    task_id="load_song_dim_table_id",
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="songs",
    sql_statement=SqlQueries.song_table_insert,
    schema="public",
    table_truncate =1 
)

load_user_dim_table = LoadDimensionOperator(
    task_id="load_user_dim_table_id",
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="users",
    sql_statement=SqlQueries.user_table_insert,
    schema="public",
    table_truncate = 1 
)

load_artist_dim_table = LoadDimensionOperator(
    task_id="load_artist_dim_table_id",
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="artists",
    sql_statement=SqlQueries.artist_table_insert,
    schema="public",
    table_truncate = 1 
)

load_time_dim_table = LoadDimensionOperator(
    task_id="load_time_dim_table_id",
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="time",
    sql_statement=SqlQueries.time_table_insert,
    schema="public",
    table_truncate = 1 
)


run_quality_checks = DataQualityOperator(
    task_id='run_quality_checks_id',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    table_column="playid",
    expected_result = 1
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_table
create_table >> stage_songs
create_table >> stage_events
stage_songs >> load_songplays_fact_table
stage_events >> load_songplays_fact_table
load_songplays_fact_table >> load_song_dim_table
load_songplays_fact_table >> load_user_dim_table
load_songplays_fact_table >> load_artist_dim_table
load_songplays_fact_table >> load_time_dim_table
load_song_dim_table >> run_quality_checks
load_user_dim_table >> run_quality_checks
load_artist_dim_table >> run_quality_checks
load_time_dim_table >> run_quality_checks
run_quality_checks >> end_operator

