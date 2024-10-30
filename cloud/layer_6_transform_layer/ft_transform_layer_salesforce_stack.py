from aws_cdk import (
    aws_lambda as lambda_,
    aws_ec2 as ec2,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_logs as logs,
    Stack,
    Duration,
    RemovalPolicy
)

from constructs import Construct

from cloud.layer_1_bootstrap.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack
from cloud.layer_2_storage.ft_decision_support_persistent_storage_stack import FtDecisionSupportPersistentStorageStack
from cloud.layer_3_core.ft_decision_support_core_stack import FtDecisionSupportCoreStack
from cloud.layer_4_ingestion_layer.ft_ingestion_layer_salesforce_stack import FtIngestionLayerSalesforceStack
from cloud.layer_5_load_layer.ft_load_layer_salesforce_stack import FtLoadLayerSalesforceStack
from cloud.shared.state_machine_alarm_generator import StateMachineAlarmGenerator
from cloud.shared.utc_time_calculator import  UTCTimeCalculator
from cloud.shared.execute_single_db_function_state_machine import ExecuteSingleDbFunctionStateMachineGenerator

from dotenv import load_dotenv
import os
import pytz
from datetime import datetime

class FtTransformLayerSalesforceStack(Stack):

    def __init__(self, 
                 scope: Construct, 
                 id: str, 
                 env: str, 
                 region: str,
                 email_addresses_to_alert_on_error: str,
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
        TRANFORMATION LAYER
        Loop through salesforce_entities
        '''
        self.lambda_execute_db_function = lambda_.Function(self, f"LambdaExecuteDbFunction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name=f"ft-{env}-execute-db-function",
            layers=[ds_core_stack.psycopg2_lambda_layer, data_core_lambda_layer],
            vpc=bootstrap_stack.decision_support_vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=bootstrap_stack.decision_support_vpc.private_subnets
            ),
            timeout=Duration.minutes(15),
            code=lambda_.Code.from_asset('cloud/lambdas/ExecuteDbFunction'),
            handler='lambda_function.lambda_handler'
        )
        # Grant the Lambda function permissions to read the DB Connection Secret
        storage_stack.db_master_user_secret.grant_read(self.lambda_execute_db_function)

        # Create an SNS topic for all of the Transform Layer
        transform_layer_alarm_topic = sns.Topic(
            self, 
            "FtTransformLayerSalesforceAlarmTopic",
            topic_name=f"ft-{env}-salesforce-tranform-layer-alarm"
        )

        # comma-delimited string of email addresses
        email_addresses = [email.strip() for email in email_addresses_to_alert_on_error.split(",") ]
        for email in email_addresses:
            # Add subscription to the SNS topic (e.g., email notification)
            transform_layer_alarm_topic.add_subscription(subs.EmailSubscription(email))

        '''
        RAW to VALID
        '''
        for salesforce_entity in ingestion_layer_stack.salesforce_entities:

            entity_name = salesforce_entity["entity_name"]

            # Load Layer - ft_ds_admin.raw_to_valid_sf_contact
            # Schedule: run every 6 hours at 3:30, 9:30, 15:30, and 21:30 EST daily
            # calculate UTC time for 3:00 EST
            utc_three = UTCTimeCalculator(hour_in_EST=3, modulo=6).calculate_utc_hour()

            ExecuteSingleDbFunctionStateMachineGenerator(
                scope=self,
                lambda_execute_db_function=self.lambda_execute_db_function,
                id_prefix=f"ft-{env}-salesforce-raw-to-valid-{entity_name}",
                db_schema="ft_ds_admin",
                db_function=f"write_sf_{entity_name.replace('-','_')}_raw_to_valid",
                db_secret_arn=storage_stack.db_master_user_secret.secret_arn,
                region=region,
                cron_schedule=events.Schedule.cron(
                    minute="30",                # At the 30th minute of the hour
                    hour=f"{str(utc_three)}/6", # 3:00 UTC, repeat every 6 hours
                    day="*",                    # Every day
                    month="*",                  # Every month
                    year="*"                    # Every year
                ),
                sns_topic=transform_layer_alarm_topic
            )
        
        '''
        VALID to REFINED
        Invoked across ALL entities.
        '''
        # Valid to Refined Transformation Layer should run every 6 hours at 5:00, 11:00, 17:00, and 23:00 EST daily
        utc_five = UTCTimeCalculator(hour_in_EST=5, modulo=6).calculate_utc_hour()

        ExecuteSingleDbFunctionStateMachineGenerator(
            scope=self,
            lambda_execute_db_function=self.lambda_execute_db_function,
            id_prefix=f"ft-{env}-salesforce-valid-to-refined",
            db_schema="ft_ds_admin",
            db_function="write_valid_to_refined",
            db_secret_arn=storage_stack.db_master_user_secret.secret_arn,
            region=region,
            cron_schedule=events.Schedule.cron(
                minute="0",                # At the 0th minute of the hour
                hour=f"{str(utc_five)}/6", # 5:00 UTC, repeat every 6 hours
                day="*",                    # Every day
                month="*",                  # Every month
                year="*"                    # Every year
            ),
            sns_topic=transform_layer_alarm_topic
        )

        '''
        HISTORICAL METRICS
        This step in the Transformation Layer should be invoked across ALL entities.
        '''
        historical_metrics = [
            {"db_function": "calculate_historical_active_participant_counts", "short_name": "active-participant-counts"},
            {"db_function": "calculate_historical_ethnic_diversity_percentage", "short_name": "ethnic-diversity-percentage"},
            {"db_function": "calculate_historical_female_percentage", "short_name": "female-percentage"},
            {"db_function": "calculate_historical_retention_percentage", "short_name": "retention-percentage"},
            {"db_function": "calculate_historical_underserved_areas_counts", "short_name": "underserved-areas-counts"},
            {"db_function": "calculate_historical_teen_retention_percentage", "short_name": "teen-retention-percentage"},
            {"db_function": "calculate_historical_twelve_up_engagement_counts", "short_name": "twelve-up-counts"},
            {"db_function": "calculate_historical_tenure_counts", "short_name": "tenure-counts"}
        ]

        # Historical Metric Functions should be run once per day at 3:00am EST, no modulo
        utc_three = UTCTimeCalculator(hour_in_EST=3).calculate_utc_hour()
        for historical_metric in historical_metrics:

            db_function = historical_metric["db_function"]
            short_name = historical_metric["short_name"]

            ExecuteSingleDbFunctionStateMachineGenerator(
                scope=self,
                lambda_execute_db_function=self.lambda_execute_db_function,
                id_prefix=f"ft-{env}-{short_name}",
                db_schema="ft_ds_admin",
                db_function=db_function,
                db_secret_arn=storage_stack.db_master_user_secret.secret_arn,
                region=region,
                cron_schedule=events.Schedule.cron(
                    minute="0",                 # At the 0th minute of the hour
                    hour=f"{str(utc_three)}",   # 3:00 UTC daily, does not repeat
                    day="*",                    # Every day
                    month="*",                  # Every month
                    year="*"                    # Every year
                ),
                sns_topic=transform_layer_alarm_topic
            )

















        # # Create Tasks for Historical Active Participant Counts
    
        # # Step 1: Call ft_ds_admin.write_valid_to_refined
        # execute_historical_active_task = tasks.LambdaInvoke(
        #     self, f"Execute Historical Metrics transformation",
        #     lambda_function=self.lambda_execute_db_function,
        #     payload=sfn.TaskInput.from_object({
        #         "db_function": f"write_valid_to_refined",
        #         "db_schema": "ft_ds_admin",
        #         "secret_arn": storage_stack.db_master_user_secret.secret_arn,
        #         "region": region
        #     }),
        #     result_path="$.valid_to_refined_result"
        # ).add_retry(
        #     max_attempts=3,
        #     interval=Duration.seconds(5),
        #     backoff_rate=2.0
        # ).add_catch(
        #     sfn.Fail(self, f"Stored Procedure raw_to_valid failed Salesforce {entity_name}", error="Stored Procedure raw_to_valid failed", cause="Stored Procedure raw_to_valid failed"),
        #     errors=["States.ALL"]
        # )

        # # Define the state machine workflow
        # definition = execute_valid_to_refined_task

        # # Create the state machine
        # valid_to_refined_state_machine = sfn.StateMachine(
        #     self, f"FtTransformStateMachineValidToRefined",
        #     state_machine_name=f"ft-{env}-transform-layer-valid-to-refined",
        #     definition=definition,
        #     timeout=Duration.minutes(60)
        # )



