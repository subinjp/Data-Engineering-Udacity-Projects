from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id,
        self.redshift_conn_id = redshift_conn_id,
        self.tables = tables

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('Executing DataQualityOperator')
        for table in self.tables:
            count = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            num_records = count[0][0]
            if len(count) < 1:
                self.log.error(f"No Data is found. {table} returned no results")
                raise ValueError(f"No Data is found. {table} returned no results")
            self.log.info(f"Data quality on table {table}  passed with {num_records} records")           
        