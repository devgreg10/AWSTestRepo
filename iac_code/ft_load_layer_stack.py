from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Stack,
    Duration
)

from constructs import Construct

from dotenv import load_dotenv
import os

class FtLoadLayerStack(Stack):

    def __init__(self, scope: Construct, id: str, env: str, secret_arn: str, secret_region: str, bucket_name: str, bucket_prefix: str, num_files: int, **kwargs) -> None:
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

        # Define Lambda functions
        lambda_list_s3_files = lambda_.Function(self, "LambdaListS3Files",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name="ft-" + env + "-s3-list-files",
            #layers=[psycopg2_layer],
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(120),
            code=lambda_.Code.from_asset('lambdas/FTDecisionSupportLoadLayer/ListS3Files'),
            handler='lambda_function.lambda_handler',
            environment={
                "BUCKET_NAME": bucket_name,
                "BUCKET_PREFIX": bucket_prefix,
                "NUM_FILES": str(num_files)  # Convert to string for Lambda environment variable
            }
        )

        # Grant the Lambda function permissions to read from S3 
        bucket.grant_read_write(lambda_list_s3_files)

        lambda_retrieve_secrets = lambda_.Function(self, "LambdaRetrieveSecrets",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name="ft-" + env + "-retrieve-secrets",
            #layers=[psycopg2_layer],
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(120),
            code=lambda_.Code.from_asset('lambdas/FTDecisionSupportLoadLayer/RetrieveSecrets'),
            handler='lambda_function.lambda_handler',
            environment={
                "DB_SECRET_ARN": secret_arn,
                "DB_SECRET_REGION": secret_region,
            }
        )

        # Grant the Lambda function permissions to read from Secrets 
        lambda_retrieve_secrets.add_to_role_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[secret_arn],
            )
        )

        lambda_process_files = lambda_.Function(self, "LambdaProcessLoadLayerFiles",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name="ft-" + env + "-load-process-files",
            layers=[psycopg2_layer],
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(120),
            code=lambda_.Code.from_asset('lambdas/FTDecisionSupportLoadLayer/ProcessFiles'),
            handler='lambda_function.lambda_handler',
            environment={
                "DB_SECRET_ARN": secret_arn,
                "DB_SECRET_REGION": secret_region,
                "BUCKET_NAME": bucket_name,
                "BUCKET_PREFIX": bucket_prefix,
                "NUM_FILES": str(num_files)  # Convert to string for Lambda environment variable
            }
        )

        # Create Tasks for State Machine

        # Step 1: Retrieve Secrets
        retrieve_secrets_task = tasks.LambdaInvoke(
            self, "Retrieve Secrets",
            lambda_function=lambda_retrieve_secrets,
            output_path="$.Payload"  # Capture the secret in the output
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "RetrieveSecretsFailed", error="RetrieveSecretsFailed", cause="Failed to retrieve secrets"),
            errors=["States.ALL"]
        )

        #Step 2: List S3 Files
        list_s3_files_task = tasks.LambdaInvoke(
            self, "List S3 Files",
            lambda_function=lambda_list_s3_files,
            output_path="$.Payload"  # Capture the output to pass it to the next step
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "ListS3FilesFailed", error="ListS3FilesFailed", cause="Failed to list files from S3"),
            errors=["States.ALL"]
        )

        # Step 3: Process Files in Batch
        process_files_task = sfn.Map(
            self, "Process Files in Batch",
            max_concurrency=5,
            items_path="$.files",  # This assumes that the list of files is passed from the previous steps
            parameters={
                "file.$": "$$.Map.Item.Value",  # Each file in the list
                "secret.$": "$.secret"  # Pass the secret to each iteration
            }
        )

        # Add Lambda invocation within the Map state
        process_files_task.iterator(tasks.LambdaInvoke(
            self, "Process File",
            lambda_function=lambda_process_files,
            result_path="$.result"  # Store the result of each iteration
        )).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "ProcessFilesFailed", error="ProcessFilesFailed", cause="Failed to process files"),
            errors=["States.ALL"]
        )

         # Define the state machine workflow
        definition = retrieve_secrets_task.next(list_s3_files_task).next(process_files_task)

        # Create the state machine
        state_machine = sfn.StateMachine(
            self, "FtLoadStateMachine",
            state_machine_name="ft-" + env + "-load-layer",
            definition=definition,
            timeout=Duration.minutes(15)
        )

        