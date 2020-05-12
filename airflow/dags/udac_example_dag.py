# import necessary libraries
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.subdag_operator import SubDagOperator
from subdags.load_dimension_subdag import load_dimension_subdag
from subdags.create_table_subdag import create_table_subdag
from helpers import SqlQueries


# configure AWS_KEY and AWS_SECRET
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')


# set default arguments
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2019, 12, 1),
    'email_on_retry': False,
    'retires': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': True
}


# define DAG
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )


start_operator = DummyOperator(task_id='Begin_execution',
                               dag=dag)


create_table_subdag = SubDagOperator(
    subdag=create_table_subdag(
        parent_dag_name="udac_example_dag",
        task_id="create_table",
        redshift_conn_id="redshift",
        start_date=datetime(2019, 1, 1)
    ),
    task_id="create_table",
    dag=dag
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    sql_stmt=SqlQueries.log_copy_command,
    provide_context=True,
    json_format="s3://udacity-dend/log_json_path.json"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    sql_stmt=SqlQueries.song_copy_command,
    json_format="auto"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_stmt=SqlQueries.songplay_table_insert
)


load_dimension_subdag = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name="udac_example_dag",
        task_id="load_dimensions",
        redshift_conn_id="redshift",
        start_date=datetime(2019, 1, 12)
    ),
    task_id="load_dimensions",
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['songs', 'time', 'users', 'artists', 'songplays'],
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_table_subdag
create_table_subdag >> stage_events_to_redshift
create_table_subdag >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >>load_songplays_table

load_songplays_table >> load_dimension_subdag

load_dimension_subdag >> run_quality_checks

run_quality_checks >> end_operator

