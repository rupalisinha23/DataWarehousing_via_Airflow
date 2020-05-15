from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_stmt,
                 mode,
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.mode = mode
        

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.mode == 'delete':
            self.log.info(f'Deleting data from {self.table} dimension table.')
            redshift.run(f'DELETE FROM {self.table};')
            self.log.info("Delete is now complete.")
        
        self.log.info(f"Loading data into {self.table} table.")
        formatted_sql = f""" INSERT INTO {self.table} ({self.sql_stmt})"""
        redshift.run(formatted_sql)
        self.log.info(f"Loading data into {self.table} table is complete.")