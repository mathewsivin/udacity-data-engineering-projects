from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    fact_sql_template = """
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
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.destination_table = destination_table
        self.sql_statement = sql_statement

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Do the query that does the thing")
        formatted_sql = LoadFactOperator.fact_sql_template.format(
            schema = self.schema,
            destination_table = self.destination_table,
            sql_statement = self.sql_statement
        )
        redshift.run(formatted_sql)    
