import psycopg2
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from data_core.util.db_execute_helper import DbExecutorHelper
from data_core.util.db_exceptions import DbException

def lambda_handler(event, context):

    db_connection = None

    # Extract necessary information from the event
    db_function = event['db_function'] 
    logging.info(f"db_function: {db_function}")
    db_schema = event['db_schema']
    logging.info(f"db_schema:  {db_schema}")
    input_parameters = event['input_parameters']
    logging.info(f"input_parameters: {input_parameters}")
    secret_arn = event['secret_arn']
    logging.info(f"secret_arn: {secret_arn}")
    region = event['region']
    logging.info(f"region: {region}")
    
    try:

        db_connection = DbExecutorHelper.get_db_connection_by_secret_arn(
            secret_arn=secret_arn,
            region=region
        )

        logging.info("Connection established")

        logging.info(f"Calling DB Function {db_schema}.{db_function}")
        response = DbExecutorHelper.execute_function(
            db_connection = db_connection,
            db_schema = db_schema, 
            db_function = db_function,
            input_parameters = input_parameters,
            commit_changes = True,
            close_db_conn = False)

    except DbException as ex:
        logging.error(f"DB Error calling Function {db_schema}.{db_function}: {ex}")
        raise ex                        

    except Exception as record_error:
        logging.error(f"Error calling DB Function {db_schema}.{db_function}: {record_error}")

    finally:
        if db_connection:
            db_connection.close()
        
        
