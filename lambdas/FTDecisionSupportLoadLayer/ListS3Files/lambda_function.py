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
        bucket_prefix = os.environ['BUCKET_PREFIX']
        num_files = int(os.environ['NUM_FILES'])
        
        logging.info("Bucket Name: " + bucket_name)
        logging.info("Bucket Prefix: " + bucket_prefix)
        
        # List the first N JSON files in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=num_files, Prefix=bucket_prefix)  # Add prefix if needed
        logging.info("Have response from s3.listobjects")
        files = response.get('Contents', [])
        logging.info(f"Found {len(files)} files in the bucket to process")
        
        file_keys = [file['Key'] for file in files]
        logging.info(f"Found files: {file_keys}")

        return {
            'files': file_keys
        }

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e