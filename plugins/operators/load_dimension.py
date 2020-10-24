
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 insert_sql="",
                 insert_sql_delete="",
                 mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql = insert_sql
        self.insert_sql_delete = insert_sql_delete
        self.mode = mode

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        logging.info("%%%%%%START%%%%%%")
        logging.info("**Set up redshift hook")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.mode == "append":
            logging.info("**Running insert sql -> {}".format(self.insert_sql))
            redshift_hook.run(self.insert_sql)
        elif self.mode == "delete":
            logging.info("**Running insert sql -> {}".format(self.insert_sql_delete))
            redshift_hook.run(self.insert_sql_delete)
        logging.info("%%%%%%END%%%%%%%%")
