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
            code=lambda_.Code.from_asset('lambda_layers/API_Layer'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8, lambda_.Runtime.PYTHON_3_9],
            description="A layer for psycopg2",
        )

        # Define the Lambda function
        lambda_function = lambda_.Function(self, "LambdaJsonToAurora",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name="ft-" + env + "-load-json-s3-to-aurora",
            layers=[psycopg2_layer],
            code=lambda_.Code.from_asset('lambdas/FTDecisionSupportLoadLayerS3ToAurora'),
            handler='lambda_function.lambda_handler',
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