import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 copy_sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.copy_sql = copy_sql
        

    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        logging.info("%%%%%%START%%%%%%")
        logging.info("**Set up redshift hook")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        logging.info("**Set up aws hook")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        logging.info("**Using aws key -> {}".format(credentials.access_key))
        logging.info("**Running copy sql -> {}".format(self.copy_sql.format(credentials.access_key, "********")))
        redshift_hook.run(self.copy_sql.format(credentials.access_key, credentials.secret_key))
        logging.info("%%%%%%END%%%%%%%%")



