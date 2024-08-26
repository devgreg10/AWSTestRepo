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
        
        file_key = event['file']
        secret = event['secret']

        # Connect to Aurora PostgreSQL
        conn = psycopg2.connect(
            host=secret['host'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password']
        )

        s3_client=session.client(
            service_name="s3",
            region_name="us-east-1"
        )
        
        bucket_name = os.environ['BUCKET_NAME']
        bucket_prefix = os.environ['BUCKET_PREFIX']

        logging.info(f"Processing file: {file_key} from bucket: {bucket_name}")
        
        try:
            with conn.cursor() as cursor:
                    
                # Get the JSON file from S3
                response = s3_client.get_object(Bucket=bucket_name, Key=file_key)

                    # Read the content
                body = response['Body'].read().decode('utf-8')

                # Process each JSON object separately
                for line in body.splitlines():
                    if line.strip():  # Skip empty lines
                        record = json.loads(line)
                        logger.info(f"Processing record: {record}")
                        cursor.execute(
                            "INSERT INTO ft_ds_raw.sf_contact " \
                            "(snapshot_date, id, otherpostalcode, chapter_affiliation__c, chapterid_contact__c, casesafeid__c, contact_type__c, age__c, ethnicity__c, gender__c, grade__c, participation_status__c) " \
                            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (formatted_timestamp, record['Id'], record['OtherPostalCode'], record['Chapter_Affiliation__c'], record['ChapterID_CONTACT__c'], record['CASESAFEID__c'], record['Contact_Type__c'], record['Age__c'], record['Ethnicity__c'], record['Gender__c'], record['Grade__c'], record['Participation_Status__c'])
                        )
                
            conn.commit()

            # Move the file to the "Complete" folder
            destination_key = f'Complete/{os.path.basename(file_key)}'
            s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': file_key}, Key=destination_key)
            s3_client.delete_object(Bucket=bucket_name, Key=file_key)

        except Exception as e:
            logger.error(f"DB Error occurred: {e}")
            conn.rollback()
            raise e
        
        finally:
            conn.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'File {file_key} processed successfully')
        }
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e