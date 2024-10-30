from aws_cdk import (
    aws_lambda as lambda_,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_ec2 as ec2,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_logs as logs,
    Stack,
    Duration,
    RemovalPolicy
)

from cloud.layer_1_bootstrap.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack
from cloud.layer_2_storage.ft_decision_support_persistent_storage_stack import FtDecisionSupportPersistentStorageStack
from cloud.layer_3_core.ft_decision_support_core_stack import FtDecisionSupportCoreStack
from cloud.layer_4_ingestion_layer.ft_ingestion_layer_salesforce_stack import FtIngestionLayerSalesforceStack
from cloud.shared.state_machine_alarm_generator import StateMachineAlarmGenerator
from cloud.shared.utc_time_calculator import  UTCTimeCalculator

from constructs import Construct

from dotenv import load_dotenv
import pytz
from datetime import datetime
import os

class FtLoadLayerSalesforceStack(Stack):

    def __init__(self, 
                 scope: Construct, 
                 id: str, 
                 env: str,
                 region: str, 
                 email_addresses_to_alert_on_error: str,
                 concurrent_lambdas: int, 
                 commit_batch_size: int,
                 bootstrap_stack: FtDecisionSupportBootstrapStack, 
                 storage_stack: FtDecisionSupportPersistentStorageStack,
                 ds_core_stack: FtDecisionSupportCoreStack,
                 ingestion_layer_stack: FtIngestionLayerSalesforceStack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        elif (env=='uat'):
            load_dotenv(os.path.join(BASEDIR,"../.env.uat"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.dev"))

        # Define a Lambda Layer for data_core
        data_core_lambda_layer = lambda_.LayerVersion(
            self, f'DataCoreLayer',
            code=lambda_.Code.from_asset('cloud/data_core_layer'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8, lambda_.Runtime.PYTHON_3_9],
            description="A layer for data_core",
        )

        '''
        LOAD LAYER - Lambda & State Machine
        Loop through salesforce_entities
        '''

        # Create an SNS topic to notify on alarm
        load_layer_alarm_topic = sns.Topic(
            self, 
            "FtLoadLayerSalesforceAlarmTopic",
            topic_name=f"ft-{env}-salesforce-load-layer-alarm"
        )

        # comma-delimited string of email addresses
        email_addresses = [email.strip() for email in email_addresses_to_alert_on_error.split(",") ]
        for email in email_addresses:
            # Add an email subscription to the SNS Topic 
            load_layer_alarm_topic.add_subscription(subscriptions.EmailSubscription(email))

        for salesforce_entity in ingestion_layer_stack.salesforce_entities:

            entity_name = salesforce_entity["entity_name"]
            salesforce_object = salesforce_entity["salesforce_object"]

            # S3 Bucket folder where this entity will be written
            s3_bucket_folder = f"salesforce/{entity_name}/ingress/"

            # Define a Lambda to list all files
            lambda_list_s3_files = lambda_.Function(self, f"LambdaListFilesSalesforce{salesforce_object}",
                runtime=lambda_.Runtime.PYTHON_3_8,
                function_name=f"ft-{env}-s3-list-salesforce-{entity_name}-files",
                vpc=bootstrap_stack.decision_support_vpc,
                vpc_subnets=ec2.SubnetSelection(
                   subnets=bootstrap_stack.decision_support_vpc.private_subnets
                ),
                timeout=Duration.seconds(30),
                code=lambda_.Code.from_asset('cloud/lambdas/ListS3Files'),
                handler='lambda_function.lambda_handler',
                environment={
                    "BUCKET_NAME": storage_stack.data_lake_bucket.bucket_name,
                    "BUCKET_FOLDER": s3_bucket_folder
                }
            )

            # Grant the Lambda function permissions to read from the Data Lake bucket 
            storage_stack.data_lake_bucket.grant_read_write(lambda_list_s3_files)
            
            # Define a Lambda to Process all Files
            lambda_process_files = lambda_.Function(self, f"LambdaProcessLoadLayerFilesSalesforce{salesforce_object}",
                runtime=lambda_.Runtime.PYTHON_3_8,
                function_name=f"ft-{env}-salesforce-{entity_name}-load-files",
                layers=[ds_core_stack.psycopg2_lambda_layer, data_core_lambda_layer],
                vpc=bootstrap_stack.decision_support_vpc,
                vpc_subnets=ec2.SubnetSelection(
                   subnets=bootstrap_stack.decision_support_vpc.private_subnets
                ),
                timeout=Duration.minutes(15),
                code=lambda_.Code.from_asset(f'cloud/lambdas/LoadLayer/Salesforce/{salesforce_object}'),
                handler='lambda_function.lambda_handler',
                environment={
                    "BUCKET_NAME": storage_stack.data_lake_bucket.bucket_name,
                    "BUCKET_FOLDER": s3_bucket_folder,
                    "COMMIT_BATCH_SIZE": commit_batch_size
                }
            )
            # Grant the Lambda function permissions to read from and write to the S3 data lake
            storage_stack.data_lake_bucket.grant_read_write(lambda_process_files)
            # Grant the Lambda function permissions to read the DB Connection Secret
            storage_stack.db_master_user_secret.grant_read(lambda_process_files)

            # Create a CloudWatch alarm based on the 'Errors' metric
            error_metric = lambda_process_files.metric_errors()

            # Define the alarm
            alarm = cloudwatch.Alarm(self, 
                                     f"LambdaLoadLayerSalesforce{salesforce_object}Alarm",
                                     alarm_name=f"ft-{env}-load-layer-salesforce-{entity_name}-error-alarm",
                                     metric=error_metric,
                                     threshold=1,  # Alarm when there is at least 1 error
                                     evaluation_periods=1,  # How many periods to evaluate before triggering
                                     datapoints_to_alarm=1)  # Number of data points before triggering alarm

            # Add an SNS action to the alarm, so the alarm triggers a notification
            alarm.add_alarm_action(cloudwatch_actions.SnsAction(load_layer_alarm_topic))

            # Create Tasks for State Machine
            
            #Step 1: List S3 Files
            list_s3_files_task = tasks.LambdaInvoke(
                self, f"List S3 Files available to ingest for {salesforce_object}",
                lambda_function=lambda_list_s3_files,
                result_path="$.files"
            ).add_retry(
                max_attempts=3,
                interval=Duration.seconds(5),
                backoff_rate=2.0
            ).add_catch(
                sfn.Fail(self, f"ListS3FilesFailedLoadLayer{entity_name}", error="ListS3FilesFailed", cause="Failed to list files from S3"),
                errors=["States.ALL"]
            )

            # Step 2: Process Files in Batch
            process_files_task = sfn.Map(
                self, f"Process Files for {salesforce_object}",
                max_concurrency=int(concurrent_lambdas),
                items_path="$.files.Payload.files",  # list of files from previous step
                parameters={
                    "region": region,
                    "file_name.$": "$$.Map.Item.Value",
                    "secret_arn": storage_stack.db_master_user_secret.secret_arn  # Pass the secret arn
                }
            )

            # Add Lambda invocation within the Map state
            process_files_task.iterator(tasks.LambdaInvoke(
                self, f"Process File for {salesforce_object}",
                lambda_function=lambda_process_files
            )).add_retry(
                max_attempts=3,
                interval=Duration.seconds(5),
                backoff_rate=2.0
            ).add_catch(
                sfn.Fail(self, f"ProcessFilesFailedLoadLayer{entity_name}", error="ProcessFilesFailed", cause="Failed to process files"),
                errors=["States.ALL"]
            )
            
            # Define the state machine workflow
            definition = list_s3_files_task.next(process_files_task)

            load_layer_log_group = logs.LogGroup(
                self, f"FtTranformLayer{entity_name}LogGroup",
                log_group_name=f"/aws/ft-{env}-salesforce-load-{entity_name}-log-group",
                retention=logs.RetentionDays.ONE_WEEK,  # Retain logs for 1 week
                removal_policy=RemovalPolicy.DESTROY
            )

            # Create the state machine
            state_machine = sfn.StateMachine(
                self, f"FtLoadStateMachineSalesforce{salesforce_object}",
                state_machine_name=f"ft-{env}-load-layer-salesforce-{entity_name}",
                definition=definition,
                timeout=Duration.hours(12),
                logs=sfn.LogOptions(
                    destination=load_layer_log_group,
                    level=sfn.LogLevel.ALL  # Log all events 
                )
            )

            # Use EventBridge Rules to trigger the state machine on a cron schedule
            # Load Layer should run every 6 hours at 3:15, 9:15, 15:15, and 21:15 EST daily

            # calculate UTC time for 3:00 EST
            utc_three = UTCTimeCalculator(hour_in_EST=3, modulo=6).calculate_utc_hour()

            rule = events.Rule(
                self, f"FtLoadSalesforce{salesforce_object}CronScheduleRule",
                    schedule=events.Schedule.cron(
                    minute="15",        # At the start of the hour (0 minutes)
                    hour=f"{str(utc_three)}/6",       # Start at 2:00 PM UTC (9am EST) and repeat every 6 hours
                    day="*",           # Every day
                    month="*",         # Every month
                    year="*"          # Every year
                ),
                rule_name=f"ft-{env}-salesforce-{entity_name}-load-cron-rule",
            )

            # Add the state machine as a target for the rule
            rule.add_target(targets.SfnStateMachine(state_machine))

            # _________________________________________
            # MONITORING - ALARM UPON ERROR or TIME OUT
            # _________________________________________
            
            StateMachineAlarmGenerator(
                scope=self,
                id_prefix=f"FtSalesforce{entity_name}Load",
                alarm_name_prefix=f"ft-{env}-salesforce-{entity_name}_load",
                log_group=load_layer_log_group,
                sns_topic=load_layer_alarm_topic,
                state_machine=state_machine
            )

            

