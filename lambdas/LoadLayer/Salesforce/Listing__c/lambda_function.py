import json
import boto3
import psycopg2
import os
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        session = boto3.session.Session()

        file_name = event['file_name']
        secret = event['secret']

        # Connect to Aurora PostgreSQL
        conn = psycopg2.connect(
            host=secret['host'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password']
        )

        s3_client = session.client(
            service_name="s3",
            region_name="us-east-1"
        )

        bucket_name = os.environ['BUCKET_NAME']
        bucket_folder = os.environ['BUCKET_FOLDER']
        commit_batch_size = int(os.environ.get('COMMIT_BATCH_SIZE', 1000))  
        error_records = []  # List to store records that fail to process

        try:
            with conn.cursor() as cursor:
                record_count = 0

                logging.info(f"Salesforce Contact Load - Processing file: {file_name} from bucket: {bucket_name}")
                
                # Get the JSON file from S3
                response = s3_client.get_object(Bucket=bucket_name, Key=file_name)

                # Stream and process each JSON object separately
                for line in response['Body'].iter_lines():
                    if line.strip():  # Skip empty lines
                        try:
                            record = json.loads(line)
                            cursor.execute(
                                "INSERT INTO ft_ds_raw.sf_contact " \
                                "(dss_last_modified_timestamp, id, mailingpostalcode, chapter_affiliation__c, chapterid_contact__c, casesafeid__c, contact_type__c, age__c, ethnicity__c, gender__c, grade__c, participation_status__c, isdeleted, lastmodifieddate, createddate) " \
                                "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (datetime.now(), record['Id'], record['MailingPostalCode'], record['Chapter_Affiliation__c'], record['ChapterID_CONTACT__c'], record['CASESAFEID__c'], record['Contact_Type__c'], record['Age__c'], record['Ethnicity__c'], record['Gender__c'], record['Grade__c'], record['Participation_Status__c'], record['IsDeleted'], record['LastModifiedDate'], record['CreatedDate'])
                            )                                

                            record_count += 1

                            # Commit after every 'n' records
                            if record_count >= commit_batch_size:
                                conn.commit()
                                #logging.info(f"Committed {record_count} records to the database.")
                                record_count = 0  # Reset the counter
                        except Exception as record_error:
                            # If there is an error processing the record, log the error with the file name
                            logging.error(f"Error processing record from file {file_name}: {line} | Error: {record_error}")
                            error_records.append({'file_name': file_name, 'line': line})

                # Move the file to the "Complete" folder
                destination_key = f'{bucket_folder}complete/{os.path.basename(file_name)}'
                if not destination_key.endswith('.json'):
                    destination_key += '.json'

                logging.info(f"Attempting to copy file | bucket_name: {bucket_name} | file_name: {file_name} | destination_key: {destination_key}")

                s3_client.copy_object(Bucket=bucket_name, 
                                        CopySource={'Bucket': bucket_name, 'Key': file_name}, 
                                        Key=destination_key)
                s3_client.delete_object(Bucket=bucket_name, Key=file_name)
                logging.info(f"File moved to 'Complete' folder: {destination_key}")

            # Commit any remaining records
            if record_count > 0:
                conn.commit()
                logging.info(f"Committed remaining {record_count} records to the database.")

        except Exception as e:
            logger.error(f"DB Error occurred: {e}")
            conn.rollback()
            raise e
        
        finally:
            conn.close()

        # If there are error records, write them to a new S3 file in the "error/" folder
        if error_records:
            error_file_name = f"{bucket_folder}error/{datetime.now().strftime('%Y%m%d_%H%M%S')}_error.json"
            error_file_content = json.dumps(error_records, indent=4)
            s3_client.put_object(Bucket=bucket_name, Key=error_file_name, Body=error_file_content)
            logging.info(f"Error records written to {error_file_name} in the 'error/' folder")

        return {
            'statusCode': 200,
            'body': json.dumps('Processing complete with error handling.')
        }

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e
