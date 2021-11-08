from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 table_column="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_column = table_column 
        #self.expected_result = expected_result
    
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        #specific data quality check for a table with null and count 
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        records_with_null = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE {self.table_column} IS NULL")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records_with_null[0][0]
        if num_records > 0:
            raise ValueError(f"Data quality check failed. {self.table} contained {records_with_null[0][0]} NULL rows")
        logging.info(f"Data quality on table {self.table} check passed with Zero NULL records")   
        
        data_quality_checks=[
           	{'table': 'public.songs',
            'test_sql': "SELECT COUNT(*) FROM public.songs",
            'expected_result': 14896},
            {'table': 'public.time',
            'test_sql': "SELECT COUNT(*) FROM public.time",
            'expected_result': 291800},
            {'table': 'public.users',
            'test_sql': "SELECT COUNT(*) FROM public.users",
            'expected_result': 104},
            {'table': 'public.artists',
            'test_sql': "SELECT COUNT(*) FROM public.artists",
            'expected_result': 10025}
        ]
        
         for table_name in data_quality_checks:
            records = redshift_hook.get_records(table_name['check_sql'])
            if len(records) != table_name['expected_result']:
                raise ValueError(f"Data quality check failed")
        else:
            logging.info("Data quality checks are completed and all tables are in correct count")
        