import boto3
import psycopg2
import os
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        session = boto3.session.Session()
        
        # Extract necessary information from the event
        procedure_name = event['procedure_name']  # Default to a procedure if not provided
        params = event.get('params', ())  # Parameters for the stored procedure, if any
        secret = event['secret']  # Secret containing DB credentials
        
        # Connect to the database
        conn = psycopg2.connect(
            host=secret['host'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password']
        )

        try:
            with conn.cursor() as cursor:
                logging.info(f"Executing stored procedure: {procedure_name} with params: {params}")
                cursor.callproc(procedure_name, params)
                result = cursor.fetchall()  # Fetch the result if the stored procedure returns data
                conn.commit()

            logging.info(f"Stored procedure executed successfully, result: {result}")

            return {
                'statusCode': 200,
                'body': result
            }

        except Exception as e:
            logging.error(f"DB Error occurred: {e}")
            conn.rollback()
            raise e
        
        finally:
            conn.close()
        
        
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        raise e