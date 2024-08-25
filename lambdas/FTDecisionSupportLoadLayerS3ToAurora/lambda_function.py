import json
import boto3
import psycopg2
import os
import logging

from botocore.exceptions import ClientError
from datetime import datetime
import pytz

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    try:

        session = boto3.session.Session()

        # Define the time zone
        eastern = pytz.timezone('America/New_York')
        
        # Get the current time
        now = datetime.now(eastern)
        
        # Format the timestamp
        formatted_timestamp = now.strftime('%Y-%m-%dT%H:%M:%S%z')
        
        # Adjust the format to include the colon in the offset
        formatted_timestamp = f"{formatted_timestamp[:-2]}:{formatted_timestamp[-2:]}"
        
        s3_client=session.client(
            service_name="s3",
            region_name="us-east-1"
        )
        
        bucket_name = os.environ['BUCKET_NAME']
        bucket_prefix = os.environ['BUCKET_PREFIX']
        num_files = int(os.environ['NUM_FILES'])
        
        logging.info("Bucket Name: " + bucket_name)
        logging.info("Bucket Prefix: " + bucket_prefix)
        
        # List the first N JSON files in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=num_files, Prefix=bucket_prefix)  # Add prefix if needed
        logging.info("Have response from s3.listobjects")
        files = response.get('Contents', [])
        logging.info("Have files")
        
        if not files:
            return {'statusCode': 200, 'body': 'No files to process'}
        
        # Log the secret ARN
        secret_arn = os.environ['DB_SECRET_ARN']
        logging.info(f"Using Secret ARN: {secret_arn}")
        secret_region = os.environ['DB_SECRET_REGION']
        logging.info(f"Using Secret Region: {secret_region}")
        
        # Retrieve the secret
        client = session.client(
            service_name='secretsmanager',
            region_name=secret_region
        )
        logging.info("1")
        secret_response = client.get_secret_value(SecretId=secret_arn)
        logging.info("2")
        secret = json.loads(secret_response['SecretString'])
        logging.info("3")

        # Connect to Aurora PostgreSQL
        conn = psycopg2.connect(
            host=secret['host'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password']
        )
        
        try:
            with conn.cursor() as cursor:
                for file in files:
                    file_key = file['Key']
                    
                    # Get the JSON file from S3
                    response = s3.get_object(Bucket=bucket_name, Key=file_key)
                    json_data = json.loads(response['Body'].read())
                    
                    # Example: Insert JSON data into the table
                    for record in json_data:
                        cursor.execute(
                            "INSERT INTO ft_ds_raw.sf_contact " \
                            "(snapshot_date, id, otherpostalcode, chapter_affiliation__c, chapterid_contact__c, casesafeid__c, contact_type__c, age__c, ethnicity__c, gender__c, grade__c, participation_status__c) " \
                            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (formatted_timestamp, record['Id'], record['OtherPostalCode'], record['Chapter_Affiliation__c'], record['ChapterID_CONTACT__c'], record['CASESAFEID__c'], record['Contact_Type__c'], record['Age__c'], record['Ethnicity__c'], record['Gender__c'], record['Grade__c'], record['Participation_Status__c'])
                        )
                    
                    # Move the file to the "Complete" folder
                    destination_key = f'Complete/{file_key.split("/")[-1]}'
                    s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': file_key}, Key=destination_key)
                    s3.delete_object(Bucket=bucket_name, Key=file_key)
                    
                conn.commit()
        
        except Exception as e:
            logger.error(f"DB Error occurred: {e}")
            conn.rollback()
            raise e
        
        finally:
            conn.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data inserted and files moved successfully')
        }
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e