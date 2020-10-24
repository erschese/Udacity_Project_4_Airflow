import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.tables = kwargs["params"]["tables_count"]
        self.tables_fields = kwargs["params"]["tables_fields_null"]

    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet')
        logging.info("%%%%%%START%%%%%%")
        logging.info("**Set up redshift hook")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # check if table field is null       
        tables_fields =  self.tables_fields
        for table_field in tables_fields:
            tmp_table_field = table_field.split(";")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {tmp_table_field[0]} WHERE {tmp_table_field[1]} IS NULL")
            if records is None or records[0][0] == 0:
                logging.info(f"NULL ENTRIES IN {tmp_table_field[0]} TABLE AND FIELD {tmp_table_field[1]}-> {records[0][0]} ")
            else:
                raise ValueError(f"NULL ENTRIES IN {tmp_table_field[0]} TABLE AND FIELD {tmp_table_field[1]}")
           
        # check if table has entries
        tables = self.tables
        for table in tables:
            logging.info(f"%%%%%%CHECK%TABLE%%%%%% {table}")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            # check records
            if records is None or records[0][0] < 1:
                raise ValueError(f"ERROR: NO RECORDS IN TABLE {table}")
            else:
                logging.info(f"ENTRIES IN {table} TABLE -> {records[0][0]} ")
         