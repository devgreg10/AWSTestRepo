from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    Stack,
    Duration
)

from constructs import Construct

from cloud.layer_1_bootstrap.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack
from cloud.layer_2_storage.ft_decision_support_persistent_storage_stack import FtDecisionSupportPersistentStorageStack
from cloud.layer_3_core.ft_decision_support_core_stack import FtDecisionSupportCoreStack
from cloud.layer_4_ingestion_layer.ft_ingestion_layer_salesforce_stack import FtIngestionLayerSalesforceStack
from cloud.layer_5_load_layer.ft_load_layer_salesforce_stack import FtLoadLayerSalesforceStack

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

            # Create the state machine
            raw_to_valid_state_machine = sfn.StateMachine(
                self, f"FtTransformStateMachineSalesforce{entity_name}",
                state_machine_name=f"ft-{env}-transform-layer-salesforce-{entity_name}",
                definition=raw_to_valid_definition,
                timeout=Duration.minutes(60)
            )

            # Use EventBridge Rules to trigger the Raw to Valid state machine on a cron schedule
            # Raw to Valid Transformation Layer should run every 6 hours at 3:30, 9:30, 15:30, and 21:30 EST daily
            est_timezone = pytz.timezone("America/New_York")
            now_timezone = datetime.now(est_timezone)

            times_est = [
                {"hour": 3, "minute": 30},
                {"hour": 9, "minute": 30},
                {"hour": 15, "minute": 30},
                {"hour": 21, "minute": 30}
            ]

            # Loop over the times and create the EventBridge rules for each
            for time_est in times_est:
                est_time = est_timezone.localize(datetime(now_timezone.year, now_timezone.month, now_timezone.day, time_est["hour"], time_est["minute"], 0))
                utc_time = est_time.astimezone(pytz.utc)

                # Create a cron rule for each converted UTC time
                rule = events.Rule(
                    self, f"FtTransformSalesforce{entity_name}{time_est['hour']}{time_est['minute']}CronRule",
                    schedule=events.Schedule.cron(
                        minute=str(utc_time.minute),
                        hour=str(utc_time.hour),
                        day="*", month="*", year="*"
                    ),
                    rule_name=f"ft-{env}-sf-{entity_name}-r2v-{time_est['hour']}-{time_est['minute']}-cron-rule"
                )

                # Add the state machine as a target for the rule
                rule.add_target(targets.SfnStateMachine(raw_to_valid_state_machine))

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

        # Create the state machine
        valid_to_refined_state_machine = sfn.StateMachine(
            self, f"FtTransformStateMachineValidToRefined",
            state_machine_name=f"ft-{env}-transform-layer-valid-to-refined",
            definition=valid_to_refined_definition,
            timeout=Duration.minutes(60)
        )

        # Use EventBridge Rules to trigger the Valid to Refined state machine on a cron schedule
        # Valid to Refined Transformation Layer should run every 6 hours at 5:00, 11:00, 17:00, and 23:00 EST daily

        times_est = [
            {"hour": 5, "minute": 00},
            {"hour": 11, "minute": 00},
            {"hour": 17, "minute": 00},
            {"hour": 23, "minute": 00}
        ]

        # Loop over the times and create the EventBridge rules for each
        for time_est in times_est:
            est_time = est_timezone.localize(datetime(now_timezone.year, now_timezone.month, now_timezone.day, time_est["hour"], time_est["minute"], 0))
            utc_time = est_time.astimezone(pytz.utc)

            # Create a cron rule for each converted UTC time
            rule = events.Rule(
                self, f"FtTransformValidToRefinedCronRule{time_est['hour']}{time_est['minute']}",
                schedule=events.Schedule.cron(
                    minute=str(utc_time.minute),
                    hour=str(utc_time.hour),
                    day="*", month="*", year="*"
                ),
                rule_name= f"ft-{env}-sf-v2r-{time_est['hour']}{time_est['minute']}-cron-rule"
            )

            # Add the state machine as a target for the rule
            rule.add_target(targets.SfnStateMachine(valid_to_refined_state_machine))

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



