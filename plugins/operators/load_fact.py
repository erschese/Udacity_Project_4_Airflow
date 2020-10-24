import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 insert_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql = insert_sql

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        logging.info("%%%%%%START%%%%%%")
        logging.info("**Set up redshift hook")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        logging.info("**Running insert sql -> {}".format(self.insert_sql))
        redshift_hook.run(self.insert_sql)
        logging.info("%%%%%%END%%%%%%%%")
