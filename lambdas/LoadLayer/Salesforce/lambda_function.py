import json
import boto3
import psycopg2
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    try:

        session = boto3.session.Session()

        batched_file_names = event['batched_files']
        secret = event['secret']
        formatted_timestamp = event['timestamp']

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
        bucket_folder = os.environ['BUCKET_FOLDER']
        commit_interval = int(os.environ.get('COMMIT_INTERVAL', 1000))  
        
        logging.info(f"Processing files: {batched_file_names} from bucket: {bucket_name}")
        
        try:
            with conn.cursor() as cursor:

                record_count = 0

                for file_name in batched_file_names:

                    # Get the JSON file from S3
                    response = s3_client.get_object(Bucket=bucket_name,  Key=file_name)
                    
                      # Stream and process each JSON object separately
                    for line in response['Body'].iter_lines():
                        if line.strip():  # Skip empty lines
                            record = json.loads(line)
                            cursor.execute(
                                "INSERT INTO ft_ds_raw.sf_contact " \
                                "(snapshot_date, id, mailingpostalcode, chapter_affiliation__c, chapterid_contact__c, casesafeid__c, contact_type__c, age__c, ethnicity__c, gender__c, grade__c, participation_status__c) " \
                                "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (formatted_timestamp, record['Id'], record['MailingPostalCode'], record['Chapter_Affiliation__c'], record['ChapterID_CONTACT__c'], record['CASESAFEID__c'], record['Contact_Type__c'], record['Age__c'], record['Ethnicity__c'], record['Gender__c'], record['Grade__c'], record['Participation_Status__c'])
                            )

                            record_count += 1

                            # Commit after every 'n' records
                            if record_count >= commit_interval:
                                conn.commit()
                                logging.info(f"Committed {record_count} records to the database.")
                                record_count = 0  # Reset the counter

                    # Move the file to the "Complete" folder
                    destination_key = f'{bucket_folder}complete/{os.path.basename(file_name)}'
                    if not destination_key.endswith('.json'):
                        destination_key += '.json'
                    s3_client.copy_object(Bucket=bucket_name, 
                                          CopySource={'Bucket': bucket_name, 'Key': file_name}, 
                                          Key=destination_key)
                    s3_client.delete_object(Bucket=bucket_name, Key=file_name)
                    #logging.info(f"File moved to 'Complete' folder: {destination_key}")

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
        
        return {
            'statusCode': 200
        }
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e