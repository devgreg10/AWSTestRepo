import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    try:

        session = boto3.session.Session()
        
        s3_client=session.client(
            service_name="s3",
            region_name="us-east-1"
        )
        
        bucket_name = os.environ['BUCKET_NAME']
        bucket_folder = os.environ['BUCKET_FOLDER']
        
        logging.info("Bucket Name: " + bucket_name)
        logging.info("Bucket Folder: " + bucket_folder)
        
        # read all files, but do not recurse beyond the Prefix provided
        all_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_folder)
        logging.info("Have response from s3.listobjects")

        files = all_files.get('Contents', [])
        # Filter out the root bucket (if present)
        filtered_files = [obj for obj in files if obj['Key'] != bucket_folder and not 'complete' in obj['Key'] and not 'error' in obj['Key']]

        logging.info(f"Found {len(filtered_files)} files in the bucket to process")
        
        file_keys = [file['Key'] for file in filtered_files]
        logging.info(f"Found files: {file_keys}")

        return {
            'files': file_keys
        }

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e