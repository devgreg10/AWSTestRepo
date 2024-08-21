from aws_cdk import (
    aws_iam as iam,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_logs as logs,
    Duration,
    Stack,
    RemovalPolicy
)

from constructs import Construct
from dotenv import load_dotenv
import os
import json

class FtEc2KeyPairLambda(Stack):

   def __init__(self, scope: Construct, construct_id: str, env: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        if (env == None):
            env = 'no_env'
        
        # Create an S3 bucket to store the private key
        key_pair_bucket = s3.Bucket(
            self, 
            "KeyPairBucket", 
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Lambda function to create EC2 key pair and store in S3
        create_key_lambda = _lambda.Function(
                                self, 
                                "ft-create-ec2-key-pair",
                                runtime=_lambda.Runtime.PYTHON_3_8,
                                handler="create_key.handler",
                                code=_lambda.Code.from_inline("""
import boto3
import os

def handler(event, context):
    ec2 = boto3.client('ec2')
    s3 = boto3.client('s3')

    key_name = event['key_name']
    bucket_name = event['bucket_name']

    # Create the key pair
    key_pair = ec2.create_key_pair(KeyName=key_name)
    private_key = key_pair['KeyMaterial']

    # Save the private key to the specified S3 bucket
    s3.put_object(
        Bucket=bucket_name,
        Key=f"{key_name}.pem",
        Body=private_key
    )

    return {
        'statusCode': 200,
        'body': f"Key pair '{key_name}' created and saved to S3 bucket '{bucket_name}'."
    }
"""),
                                             environment={
                                                 'BUCKET_NAME': key_pair_bucket.bucket_name,
                                             },
                                             timeout=Duration.minutes(1),
                                             log_retention=logs.RetentionDays.ONE_WEEK)

        # Grant the Lambda function permissions to create key pairs and write to S3
        create_key_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ec2:CreateKeyPair", "s3:PutObject"],
                resources=["*"]
            )
        )

        # Grant the Lambda function permission to write to the specific S3 bucket
        key_pair_bucket.grant_put(create_key_lambda)