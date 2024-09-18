import json
import boto3
import os
import logging
from datetime import datetime
from data_core.salesforce.listing.sf_listing_db_helper import SalesforceListingDbHelper
from data_core.salesforce.listing.sf_listing_db_models import SfListingSourceModel
from data_core.util.db_execute_helper import DbExecutorHelper
from data_core.util.db_exceptions import DbException, DbErrorCode

from attr import asdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    db_connection = None

    function_name = "Load layer lambda for Salesforce Listing"

    try:
        session = boto3.session.Session()

        secret_arn = event['secret_arn']
        region = event['region']
        file_name = event['file_name']

        s3_client = session.client(
            service_name="s3",
            region_name=region
        )

        bucket_name = os.environ['BUCKET_NAME']
        bucket_folder = os.environ['BUCKET_FOLDER']
        commit_batch_size = int(os.environ.get('COMMIT_BATCH_SIZE', 1000))  
        error_records = []  # List to store records that fail to process

        db_connection = None

        try:

            db_connection = DbExecutorHelper.get_db_connection_by_secret_arn(
                secret_arn=secret_arn,
                region=region
            )

            logging.info(f"{function_name} - Processing file: {file_name} from bucket: {bucket_name}")

            # Get the JSON file from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=file_name)

            chunk = []

            try:

                # Stream and process each JSON object separately
                for line in response['Body'].iter_lines():

                    if line.strip():  # Skip empty lines

                        # parse the json line into a dictionary
                        listing_json = json.loads(line)

                        # Create the SfListingSourceModel object from the dictionary
                        sf_source_listing = SfListingSourceModel(**listing_json)

                        # logging.info(f"SfListingSourceModel: {json.dumps(asdict(sf_source_listing), indent=4)}")

                        # Add the listing to the current chunk
                        chunk.append(sf_source_listing)

                        # Once the commit_batch_size is met, process the chunk and reset the list
                        if len(chunk) == commit_batch_size:
                            
                            logging.info(f"{function_name} - calling SalesforceListingDbHelper.insert_sf_raw_listings_from_source_listings for chunk size {commit_batch_size}")
                            SalesforceListingDbHelper.insert_sf_raw_listings_from_source_listings(
                                db_connection = db_connection,
                                source_listings = chunk,
                                commit_changes = True)
                                
                            # reset chunk upon commit
                            chunk = []
                
                # Process any remaining listings that didn't fill the last chunk and close the DB connection
                if chunk:
                    logging.info(f"{function_name} - calling SalesforceListingDbHelper.insert_sf_raw_listings_from_source_listings for chunk size {len(chunk)}")
                    SalesforceListingDbHelper.insert_sf_raw_listings_from_source_listings(
                        db_connection = db_connection,
                        source_listings = chunk,
                        commit_changes = True)              
                            
            except DbException as ex:
                if ex.error_code == DbErrorCode.NATURAL_KEY_VIOLATION.value:
                    logging.error(f"{function_name} - Record already exists from file {file_name}: {line} | Error: {record_error}")
                else:
                    raise ex                        

            except Exception as record_error:
                # If there is an error processing the record, log the error with the file name
                logging.error(f"{function_name} - Error processing record from file {file_name}: {line} | Error: {record_error}")
                error_records.append({'file_name': file_name, 'line': line})

            # Move the file to the "Complete" folder
            destination_key = f'{bucket_folder}complete/{os.path.basename(file_name)}'
            if not destination_key.endswith('.json'):
                destination_key += '.json'

            logging.info(f"{function_name} - Attempting to copy file | bucket_name: {bucket_name} | file_name: {file_name} | destination_key: {destination_key}")

            s3_client.copy_object(Bucket=bucket_name, 
                                    CopySource={'Bucket': bucket_name, 'Key': file_name}, 
                                    Key=destination_key)
            s3_client.delete_object(Bucket=bucket_name, Key=file_name)
            logging.info(f"{function_name} - File moved to 'Complete' folder: {destination_key}")
            
        except Exception as e:
            logger.error(f"{function_name} - Error occurred loading S3 listings to Raw: {e}")
            if db_connection:
                db_connection.rollback()
            raise e
        
        finally:

            # Close the DB Connection
            if db_connection:
                db_connection.close()

        # If there are error records, write them to a new S3 file in the "error/" folder
        if error_records:
            error_file_name = f"{bucket_folder}error/{datetime.now().strftime('%Y%m%d_%H%M%S')}_error.json"
            error_records = decode_bytes(error_records)
            error_file_content = json.dumps(error_records, indent=4)
            s3_client.put_object(Bucket=bucket_name, Key=error_file_name, Body=error_file_content)
            logging.info(f"{function_name} - Error records written to {error_file_name} in the 'error/' folder")

        return {
            'statusCode': 200,
            'body': json.dumps(f"{function_name} - Processing complete with error handling.")
        }

    except Exception as e:
        logger.error(f"{function_name} - Error occurred: {e}")
        raise e
    


def decode_bytes(data):
    """Recursively decode byte objects in a data structure (list/dict)."""
    if isinstance(data, dict):
        return {k: decode_bytes(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [decode_bytes(i) for i in data]
    elif isinstance(data, bytes):
        return data.decode('utf-8')  # or another encoding, if needed
    else:
        return data
