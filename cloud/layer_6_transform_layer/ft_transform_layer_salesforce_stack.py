from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_logs as logs,
    Stack,
    Duration
)

from constructs import Construct

from cloud.layer_1_bootstrap.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack
from cloud.layer_2_storage.ft_decision_support_persistent_storage_stack import FtDecisionSupportPersistentStorageStack
from cloud.layer_3_core.ft_decision_support_core_stack import FtDecisionSupportCoreStack
from cloud.layer_4_ingestion_layer.ft_ingestion_layer_salesforce_stack import FtIngestionLayerSalesforceStack
from cloud.layer_5_load_layer.ft_load_layer_salesforce_stack import FtLoadLayerSalesforceStack
from cloud.shared.state_machine_alarm_generator import StateMachineAlarmGenerator

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
                 ingestion_layer_stack: FtIngestionLayerSalesforceStack, 
                 load_layer_stack: FtLoadLayerSalesforceStack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        elif (env=='uat'):
            load_dotenv(os.path.join(BASEDIR,"../.env.uat"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.dev"))

        '''
        TRANFORMATION LAYER
        Loop through salesforce_entities
        '''

        self.lambda_execute_db_function = lambda_.Function(self, f"LambdaExecuteDbFunction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name=f"ft-{env}-execute-db-function",
            layers=[ds_core_stack.psycopg2_lambda_layer, load_layer_stack.data_core_lambda_layer],
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

        '''
        RAW to VALID
        '''

        # Create an SNS topic to notify on alarm
        alarm_topic = sns.Topic(
                        self, 
                        "FtTransformLayerSalesforceAlarmTopic",
                        topic_name=f"ft-{env}-salesforce-tranform-layer-alarm")
        
        # comma-delimited string of email addresses
        email_addresses = [email.strip() for email in email_addresses_to_alert_on_error.split(",") ]
        for email in email_addresses:
            # Add subscription to the SNS topic (e.g., email notification)
            alarm_topic.add_subscription(subs.EmailSubscription(email))

        # Define the parameters as a dictionary
        raw_to_valid_params = {}

        for salesforce_entity in ingestion_layer_stack.salesforce_entities:

            entity_name = salesforce_entity["entity_name"]

            # Create Tasks for Raw to Valid State Machine
        
            # Step 1: Call ft_ds_admin.raw_to_valid_sf_contact
            execute_raw_to_valid_task = tasks.LambdaInvoke(
                self, f"Execute Raw to Valid Salesforce {entity_name}",
                lambda_function=self.lambda_execute_db_function,
                payload=sfn.TaskInput.from_object({
                    "db_function": f"write_sf_{entity_name.replace('-','_')}_raw_to_valid",
                    "db_schema": "ft_ds_admin",
                    "input_parameters": raw_to_valid_params,
                    "secret_arn": storage_stack.db_master_user_secret.secret_arn,
                    "region": region
                }),
                result_path="$.raw_to_valid_result"
            ).add_retry(
                max_attempts=3,
                interval=Duration.seconds(5),
                backoff_rate=2.0
            ).add_catch(
                sfn.Fail(self, f"Stored Procedure raw_to_valid failed Salesforce {entity_name}", error="Stored Procedure raw_to_valid failed", cause="Stored Procedure raw_to_valid failed"),
                errors=["States.ALL"]
            )

            # Define the state machine workflow
            raw_to_valid_definition = execute_raw_to_valid_task

            raw_to_valid_log_group = logs.LogGroup(
                self, f"FtTransformStateMachineSalesforce{entity_name}LogGroup",
                log_group_name=f"/aws/ft-{env}-salesforce-raw-to-valid-{entity_name}",
                retention=logs.RetentionDays.ONE_WEEK  # Retain logs for 1 week
            )

            # Create the state machine
            raw_to_valid_state_machine = sfn.StateMachine(
                self, f"FtTransformStateMachineSalesforce{entity_name}",
                state_machine_name=f"ft-{env}-salesforce-raw-to-valid-{entity_name}",
                definition=raw_to_valid_definition,
                timeout=Duration.minutes(60),
                logs=sfn.LogOptions(
                    destination=raw_to_valid_log_group,
                    level=sfn.LogLevel.ALL  # Log all events 
                )
            )

            # ___________________________________
            # TRIGGER STATE MACHINE ON A SCHEDULE
            # ___________________________________

            # Create a cron rule for each converted UTC time
            rule = events.Rule(
                self, f"FtTransformSalesforce{entity_name}CronRule",
                schedule=events.Schedule.cron(
                    minute="30",       # At the 30th minute of the hour
                    hour="14/6",       # Start at 2:30 PM UTC (9am EST) and repeat every 6 hours
                    day="*",           # Every day
                    month="*",         # Every month
                    year="*"          # Every year
                ),
                rule_name=f"ft-{env}-sf-{entity_name}-raw-to-valid-cron-rule"
            )

            # Add the state machine as a target for the rule
            rule.add_target(targets.SfnStateMachine(raw_to_valid_state_machine))

            # _________________________________________
            # MONITORING - ALARM UPON ERROR or TIME OUT
            # _________________________________________
            
            StateMachineAlarmGenerator(
                scope=self,
                id_prefix=f"FtSalesforce{entity_name}RawToValid",
                alarm_name_prefix=f"ft-{env}-salesforce-{entity_name}-raw-to-valid",
                log_group=raw_to_valid_log_group,
                sns_topic=alarm_topic,
                state_machine=raw_to_valid_state_machine
            )

        '''
        VALID to REFINED
        This step in the Transformation Layer should be invoked across ALL entities.
        '''
        # Create Tasks for Valid to Refined State Machine
    
        # Step 1: Call ft_ds_admin.write_valid_to_refined
        execute_valid_to_refined_task = tasks.LambdaInvoke(
            self, f"Execute Valid to Refined transformation",
            lambda_function=self.lambda_execute_db_function,
            payload=sfn.TaskInput.from_object({
                "db_function": f"write_valid_to_refined",
                "db_schema": "ft_ds_admin",
                "secret_arn": storage_stack.db_master_user_secret.secret_arn,
                "region": region
            }),
            result_path="$.valid_to_refined_result"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, f"Stored Procedure valid-to-refined failed", error="Stored Procedure valid-to-refined failed", cause="Stored Procedure valid-to-refined failed"),
            errors=["States.ALL"]
        )

        # Define the state machine workflow
        valid_to_refined_definition = execute_valid_to_refined_task

        valid_to_refined_log_group = logs.LogGroup(
            self, "FtTransformValidToRefinedLogGroup",
            log_group_name=f"/aws/ft-{env}-valid-to-refined-log-group",
            retention=logs.RetentionDays.ONE_WEEK  # Retain logs for 1 week
        )

        # Create the state machine
        valid_to_refined_state_machine = sfn.StateMachine(
            self, f"FtTransformStateMachineValidToRefined",
            state_machine_name=f"ft-{env}-salesforce-valid-to-refined",
            definition=valid_to_refined_definition,
            timeout=Duration.minutes(60),
            logs=sfn.LogOptions(
                destination=valid_to_refined_log_group,
                level=sfn.LogLevel.ALL  # Log all events 
            )
        )

        # ___________________________________
        # TRIGGER STATE MACHINE ON A SCHEDULE
        # ___________________________________

        # Use EventBridge Rules to trigger the Valid to Refined state machine on a cron schedule
        # Valid to Refined Transformation Layer should run every 6 hours at 5:00, 11:00, 17:00, and 23:00 EST daily
       
        # Create a cron rule for each converted UTC time
        rule = events.Rule(
            self, f"FtTransformValidToRefinedCronRule",
            schedule=events.Schedule.cron(
                minute="0",        # At the start of the hour (0 minutes)
                hour="16/6",       # Start at 4:00 PM UTC (11am EST) and repeat every 6 hours
                day="*",           # Every day
                month="*",         # Every month
                year="*"          # Every year
            ),
            rule_name= f"ft-{env}-salesforce-valid-to-refined-cron-rule"
        )

        # Add the state machine as a target for the rule
        rule.add_target(targets.SfnStateMachine(valid_to_refined_state_machine))

        # _________________________________________
        # MONITORING - ALARM UPON ERROR or TIME OUT
        # _________________________________________
        
        StateMachineAlarmGenerator(
            scope=self,
            id_prefix=f"FtSalesforceValidToRefined",
            alarm_name_prefix=f"ft-{env}-salesforce-valid-to-refined",
            log_group=valid_to_refined_log_group,
            sns_topic=alarm_topic,
            state_machine=valid_to_refined_state_machine
        )

        # '''
        # HISTORICAL METRICS
        # This step in the Transformation Layer should be invoked across ALL entities.
        # '''
        # # Create Tasks for Historical Metrics State Machine
    
        # # Step 1: Call ft_ds_admin.write_valid_to_refined
        # execute_valid_to_refined_task = tasks.LambdaInvoke(
        #     self, f"Execute Valid to Refined transformation",
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



