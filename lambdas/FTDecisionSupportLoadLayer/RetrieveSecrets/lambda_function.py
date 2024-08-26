import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    try:

        session = boto3.session.Session()

        # Log the secret ARN
        secret_arn = os.environ['DB_SECRET_ARN']
        logging.info(f"Using Secret ARN: {secret_arn}")
        secret_region = os.environ['DB_SECRET_REGION']
        logging.info(f"Using Secret Region: {secret_region}")
        
        # Retrieve the secret
        secret_client = session.client(
            service_name='secretsmanager',
            region_name=secret_region
        )
        secret_response = secret_client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(secret_response['SecretString'])
        logging.info("Secret retrieved successfully")

        return{
            'secret': secret
        }

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e