from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    Stack
)

from constructs import Construct

class FtS3ToAuroraLoadStack(Stack):

    def __init__(self, scope: Construct, id: str, env: str, secret_arn: str, bucket_name: str, bucket_prefix: str, num_files: int, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Define the S3 bucket where the JSON files are stored
        bucket = s3.Bucket.from_bucket_name(self, "MyBucket", bucket_name)

        # Define a Lambda Layer for the psycopg2 library
        psycopg2_layer = lambda_.LayerVersion(
            self, 'Psycopg2Layer',
            code=lambda_.Code.from_asset('lambda_layers/lambda_layer_v1.zip'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8, lambda_.Runtime.PYTHON_3_9],
            description="A layer for psycopg2",
        )

        # Define the Lambda function
        lambda_function = lambda_.Function(self, "LambdaJsonToAurora",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name="ft-" + env + "-load-json-s3-to-aurora",
            handler="lambda_function.lambda_handler",
            layers=[psycopg2_layer],
            code=lambda_.Code.from_inline(
                """
                import json
                import boto3
                import psycopg2
                import os
                import logging

                logger = logging.getLogger()
                logger.setLevel(logging.INFO)

                def lambda_handler(event, context):
                    # Log the secret ARN
                    secret_arn = os.environ['DB_SECRET_ARN']
                    logger.info(f"Using Secret ARN: {secret_arn}")
                    
                    s3 = boto3.client('s3')
                    bucket_name = os.environ['BUCKET_NAME']
                    bucket_prefix = os.environ['BUCKET_PREFIX']
                    num_files = int(os.environ['NUM_FILES'])
                    
                    # List the first N JSON files in the bucket
                    response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=num_files, Prefix=bucket_prefix)  # Add prefix if needed
                    files = response.get('Contents', [])
                    
                    if not files:
                        return {'statusCode': 200, 'body': 'No files to process'}
                    
                    # Retrieve the secret
                    client = boto3.client('secretsmanager')
                    secret_response = client.get_secret_value(SecretId=secret_arn)
                    secret = json.loads(secret_response['SecretString'])
                    
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
                                        "INSERT INTO your_table_name (column1, column2, column3) VALUES (%s, %s, %s)",
                                        (record['field1'], record['field2'], record['field3'])
                                    )
                                
                                # Move the file to the "Complete" folder
                                destination_key = f'Complete/{file_key.split("/")[-1]}'
                                s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': file_key}, Key=destination_key)
                                s3.delete_object(Bucket=bucket_name, Key=file_key)
                                
                            conn.commit()
                    
                    except Exception as e:
                        conn.rollback()
                        raise e
                    
                    finally:
                        conn.close()
                    
                    return {
                        'statusCode': 200,
                        'body': json.dumps('Data inserted and files moved successfully')
                    }
                """
            ),
            environment={
                "DB_SECRET_ARN": secret_arn,
                "BUCKET_NAME": bucket_name,
                "BUCKET_PREFIX": bucket_prefix,
                "NUM_FILES": str(num_files)  # Convert to string for Lambda environment variable
            }
        )

        # Grant the Lambda function permissions to read from S3 and Secrets Manager
        bucket.grant_read_write(lambda_function)
        lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[secret_arn],
            )
        )