from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    Stack,
    Duration
)

from constructs import Construct

from dotenv import load_dotenv
import os

class FtS3ToAuroraLoadStack(Stack):

    def __init__(self, scope: Construct, id: str, env: str, secret_arn: str, secret_region: str, bucket_name: str, bucket_folder: str, num_files: int, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.non_prod"))

        vpc_name = os.getenv('vpc_name')
        vpc_id = os.getenv('vpc_id')
        public_subnets = [ec2.Subnet.from_subnet_attributes(
                            self, 
                            "PublicSubnetA", 
                            subnet_id=os.getenv('public_subnet_a'),
                            availability_zone=os.getenv("public_subnet_a_az")
                          )]
        private_subnets = [ec2.Subnet.from_subnet_id(self, "PrivateSubnetA", os.getenv('private_subnet_a')),
                           ec2.Subnet.from_subnet_id(self, "PrivateSubnetD", os.getenv('private_subnet_d'))]

        # Get a reference to the existing VPC
        datawarehouse_vpc = ec2.Vpc.from_vpc_attributes(
            self,
            "ExistingVPC",
            vpc_id=vpc_id,
            availability_zones=["us-east-1a"]
        )

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
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(120),
            code=lambda_.Code.from_asset('lambdas/FTDecisionSupportLoadLayerS3ToAurora'),
            handler='lambda_function.lambda_handler',
            environment={
                "DB_SECRET_ARN": secret_arn,
                "DB_SECRET_REGION": secret_region,
                "BUCKET_NAME": bucket_name,
                "BUCKET_FOLDER": bucket_folder,
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