from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_secretsmanager as secretsmanager,
    Stack,
    Duration
)

from iac_code.ft_ingestion_layer_salesforce_contact_stack import FtSalesforceContactIngestionLayerStack
from iac_code.ft_create_secret import FtCreateSecretsStack

from constructs import Construct

from dotenv import load_dotenv
import os

class FtLoadLayerSalesforceContactStack(Stack):

    def __init__(self, 
                 scope: Construct, 
                 id: str, 
                 env: str, 
                 bucket_folder: str,
                 concurrent_lambdas: int, 
                 commit_batch_size: int, 
                 secret_region: str,
                 secret_layer_stack: FtCreateSecretsStack,
                 ingestion_layer_stack: FtSalesforceContactIngestionLayerStack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        elif (env=='uat'):
            load_dotenv(os.path.join(BASEDIR,"../.env.uat"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.dev"))

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

        # Define a Lambda Layer for the psycopg2 library
        psycopg2_layer = lambda_.LayerVersion(
            self, 'Psycopg2Layer',
            code=lambda_.Code.from_asset('lambda_layers/API_Layer'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8, lambda_.Runtime.PYTHON_3_9],
            description="A layer for psycopg2",
        )

        # Define Lambda functions
        lambda_generate_timestamp = lambda_.Function(
            self, "LambdaGenerateTimestamp",
            layers=[psycopg2_layer],
            function_name=f"ft-{env}-generate-timestamp",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset('lambdas/GenerateTimestamp'),
            handler='lambda_function.lambda_handler',
            timeout=Duration.seconds(10)
        )

        
        lambda_list_s3_files = lambda_.Function(self, "LambdaListS3Files",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name=f"ft-{env}-s3-list-files",
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(30),
            code=lambda_.Code.from_asset('lambdas/ListS3Files'),
            handler='lambda_function.lambda_handler',
            environment={
                "BUCKET_NAME": ingestion_layer_stack.data_lake_bucket.bucket_name,
                "BUCKET_FOLDER": bucket_folder
            }
        )

        # Grant the Lambda function permissions to read from the Data Lake bucket 
        ingestion_layer_stack.data_lake_bucket.grant_read_write(lambda_list_s3_files)
        
        self.lambda_retrieve_secrets = lambda_.Function(self, "LambdaRetrieveSecrets",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name=f"ft-{env}-retrieve-secrets",
            #layers=[psycopg2_layer],
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(30),
            code=lambda_.Code.from_asset('lambdas/RetrieveSecrets'),
            handler='lambda_function.lambda_handler',
            environment={
                "DB_SECRET_ARN": secret_layer_stack.db_secret.secret_arn,
                "DB_SECRET_REGION": secret_region,
            }
        )

        # Grant the Lambda function permission to retrieve the DB secret
        secret_layer_stack.db_secret.grant_read(self.lambda_retrieve_secrets)

        lambda_process_files = lambda_.Function(self, "LambdaProcessLoadLayerFiles",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name=f"ft-{env}-salesforce-contact-load-files",
            layers=[psycopg2_layer],
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.minutes(15),
            code=lambda_.Code.from_asset('lambdas/LoadLayer/Salesforce/Contact'),
            handler='lambda_function.lambda_handler',
            environment={
                "BUCKET_NAME": ingestion_layer_stack.data_lake_bucket.bucket_name,
                "BUCKET_FOLDER": bucket_folder,
                "COMMIT_BATCH_SIZE": commit_batch_size
            }
        )
         # Grant the Lambda function permissions to read from and write to the S3 data lake
        ingestion_layer_stack.data_lake_bucket.grant_read_write(lambda_process_files)

        # Create Tasks for State Machine

        # Step 0: GenerateTimestamp
        generate_timestamp_task = tasks.LambdaInvoke(
            self, "Generate Timestamp",
            lambda_function=lambda_generate_timestamp,
            result_path="$.timestamp"  # Store the generated timestamp in $.timestamp
        )

        # Step 1: Retrieve Secrets
        retrieve_secrets_task = tasks.LambdaInvoke(
            self, "Retrieve Secrets",
            lambda_function=self.lambda_retrieve_secrets,
            result_path="$.secret"
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
            self, "List S3 Files available to ingest",
            lambda_function=lambda_list_s3_files,
            result_path="$.files"
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
            self, "Process Files",
            max_concurrency=concurrent_lambdas,
            items_path="$.files.Payload.files",  # list of files from previous step
            parameters={
                "file_name.$": "$$.Map.Item.Value",
                "secret.$": "$.secret.Payload.secret",  # Pass the secret to each iteration
                "timestamp.$": "$.timestamp.Payload.timestamp"  # Pass the timestamp to each iteration
            }
        )

        # Add Lambda invocation within the Map state
        process_files_task.iterator(tasks.LambdaInvoke(
            self, "Process File",
            lambda_function=lambda_process_files
        )).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "ProcessFilesFailed", error="ProcessFilesFailed", cause="Failed to process files"),
            errors=["States.ALL"]
        )
        
         # Define the state machine workflow
        definition = generate_timestamp_task.next(retrieve_secrets_task).next(list_s3_files_task).next(process_files_task)

        # Create the state machine
        state_machine = sfn.StateMachine(
            self, "FtLoadStateMachine",
            state_machine_name=f"ft-{env}-load-layer-salesforce-contact",
            definition=definition
        )

        
        # Create an EventBridge rule to capture the AppFlow run complete event
        appflow_event_rule = events.Rule(
            self, "AppFlowRunCompleteRule",
            rule_name=f"ft-{env}-ingestion-layer-complete",
            event_pattern= events.EventPattern(
                source=["aws.appflow"],
                detail_type=["AppFlow Flow Execution Status"],
                detail={
                    "status": ["SUCCEEDED"],  # This matches successful flow executions
                    "flow-name": [f"ft-{env}-ingestion-layer-salesforce-contact"]  # ZZZ - replace when the stack is passed in correctly
                }
            )
        ) 

        # Set the target of the rule to the Step Functions state machine
        appflow_event_rule.add_target(targets.SfnStateMachine(state_machine))

        