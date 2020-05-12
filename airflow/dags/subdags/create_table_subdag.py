from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
# from airflow import sql_statements
from helpers import SqlQueries


def create_table_subdag(parent_dag_name,
                          task_id,
                          redshift_conn_id,
                          *args, **kwargs):
    dag = DAG(
            f"{parent_dag_name}.{task_id}",
        **kwargs)
    
    create_staging_events_table = PostgresOperator(
        task_id="create_staging_events_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_STAGING_EVENTS_TABLE_SQL)

    create_staging_songs_table = PostgresOperator(
        task_id="create_staging_songs_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_STAGING_SONGS_TABLE_SQL)
        
    create_songplays_table = PostgresOperator(
        task_id="create_songplays_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_SONGPLAYS_TABLE_SQL)
           
    create_artists_table = PostgresOperator(
        task_id="create_artists_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_ARTISTS_TABLE_SQL)
        
    create_songs_table = PostgresOperator(
        task_id="create_songs_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_SONGS_TABLE_SQL)
        
    create_time_table = PostgresOperator(
        task_id="create_time_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_TIME_TABLE_SQL)    
    
    create_users_table = PostgresOperator(
        task_id="create_users_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_USERS_TABLE_SQL) 
         

    create_staging_events_table
    create_staging_songs_table
    create_users_table
    create_artists_table
    create_time_table
    create_songs_table
    create_songplays_table
    
    return dag