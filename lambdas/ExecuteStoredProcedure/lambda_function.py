import psycopg2
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        
        # Extract necessary information from the event
        procedure_name = event['procedure_name'] 
        logging.info(f"Stored Procedure name {procedure_name} passed to Lambda")
        params = event.get('params', ())  # Parameters for the stored procedure, if any
        secret = event['secret']  # Secret containing DB credentials
        
        # If the secret is a string, parse it into a dictionary
        if isinstance(secret, str):
            secret = json.loads(secret)

        # Connect to the database
        conn = psycopg2.connect(
            host=secret['host'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password']
        )

        try:
            with conn.cursor() as cursor:
                # Construct the CALL statement
                if params:
                    # If there are parameters, construct the call with them
                    placeholders = ', '.join(['%s'] * len(params))
                    call_statement = f"CALL {procedure_name}({placeholders})"
                    logging.info(f"Executing: {call_statement} with params: {params}")
                    cursor.execute(call_statement, params)
                else:
                    # If there are no parameters, construct a simple CALL
                    call_statement = f"CALL {procedure_name}()"
                    logging.info(f"Executing: {call_statement}")
                    cursor.execute(call_statement)

                conn.commit()
                
                logging.info(f"Successfully called procedure: {procedure_name}")
                return {'result': f"Procedure {procedure_name} executed successfully"}
            
        except Exception as e:
            logging.error(f"DB Error occurred: {e}")
            conn.rollback()
            raise e
        
        finally:
            conn.close()
        
        
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        raise e