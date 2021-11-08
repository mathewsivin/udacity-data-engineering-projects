from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    facts_truncate_sql_template = """
    TRUNCATE TABLE {schema}.{destination_table} ; 
        """
    facts_sql_template = """
    INSERT INTO  {schema}.{destination_table}
    {sql_statement} 
    ;
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 schema="",
                 sql_statement="",
                 table_truncate=1,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.destination_table = destination_table
        self.sql_statement = sql_statement
        self.table_truncate = table_truncate 
        
    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Do the query that does the thing")
        formattedtruncate_sql = LoadDimensionOperator.facts_truncate_sql_template.format(
            schema = self.schema,
            destination_table = self.destination_table
        )
        formatted_sql = LoadDimensionOperator.facts_sql_template.format(
            schema = self.schema,
            destination_table = self.destination_table,
            sql_statement = self.sql_statement
        )
        
        if self.table_truncate == 1: 
            redshift.run(formattedtruncate_sql)   
        redshift.run(formatted_sql)                