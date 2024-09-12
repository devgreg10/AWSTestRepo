from typing import List

from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_appflow as appflow,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_secretsmanager as secretsmanager,
    Stack,
    Duration
)

from datetime import datetime, timedelta, timezone

from constructs import Construct

from dotenv import load_dotenv

from iac_code.ft_decision_support_base_stack import FtDecisionSupportBaseStack

import os

class FtSalesforceEntityStack(Stack):

    def __init__(self, 
                 scope: Construct, 
                 id: str, 
                 env: str, 
                 entity_name: str,
                 salesforce_object: str,
                 app_flow_tasks: List[appflow.CfnFlow.TaskProperty],
                 commit_batch_size: int,
                 concurrent_lambdas: int,
                 ds_base_stack: FtDecisionSupportBaseStack,
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        elif (env=='uat'):
            load_dotenv(os.path.join(BASEDIR,"../.env.uat"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.dev"))


        # S3 Bucket folder where this entity will be written
        s3_bucket_folder = f"salesforce/{entity_name}/"

        '''
        INGESTION LAYER - AppFlow
        '''
        # AppFlow will be scheduled to start 15 min from now, so get the current time in UTC
        now = datetime.now(timezone.utc)

        # Add 15 minutes to the current time
        future_time = now + timedelta(minutes=15)

        # Convert the future time to a UNIX timestamp
        unix_timestamp = int(future_time.timestamp())
       
        # Create the AppFlow flow
        self.ingestion_appflow = appflow.CfnFlow(
            self, f"IngestionLayerSalesforceAppFlow{salesforce_object}",
            flow_name=f"ft-{env}-ingestion-layer-salesforce-{entity_name}",
            source_flow_config=appflow.CfnFlow.SourceFlowConfigProperty(
                connector_type="Salesforce",
                connector_profile_name=os.getenv('salesforce_connection_name'),
                source_connector_properties=appflow.CfnFlow.SourceConnectorPropertiesProperty(
                    salesforce=appflow.CfnFlow.SalesforceSourcePropertiesProperty(
                        object=salesforce_object,
                        enable_dynamic_field_update=False,
                        include_deleted_records=True,
                        data_transfer_api="AUTOMATIC"
                    )
                ),
                incremental_pull_config=appflow.CfnFlow.IncrementalPullConfigProperty(
                    datetime_type_field_name="LastModifiedDate"
                )
            ),
            destination_flow_config_list=[appflow.CfnFlow.DestinationFlowConfigProperty(
                connector_type="S3",
                destination_connector_properties=appflow.CfnFlow.DestinationConnectorPropertiesProperty(
                    s3=appflow.CfnFlow.S3DestinationPropertiesProperty(
                        bucket_name=ds_base_stack.data_lake_bucket.bucket_name,
                        bucket_prefix=f"{s3_bucket_folder}ingress",
                        s3_output_format_config=appflow.CfnFlow.S3OutputFormatConfigProperty(
                            aggregation_config=appflow.CfnFlow.AggregationConfigProperty(
                                aggregation_type="None",
                                target_file_size=64
                            ),
                            file_type="JSON",
                            prefix_config=appflow.CfnFlow.PrefixConfigProperty(
                                prefix_format="MINUTE", # Determines the level of granularity for the date and time that's included in the prefix.
                                prefix_type="FILENAME" # Determines the format of the prefix, and whether it applies to the file name, file path, or both.
                            ),
                            preserve_source_data_typing=False # all source data converted into strings
                        )
                    )
                )
            )],
            trigger_config=appflow.CfnFlow.TriggerConfigProperty(
                trigger_type="Scheduled",
                trigger_properties=appflow.CfnFlow.ScheduledTriggerPropertiesProperty(
                    schedule_expression="rate(60minutes)",
                    data_pull_mode="Incremental",
                    schedule_start_time=unix_timestamp,
                    time_zone="America/New_York",
                    schedule_offset=0
                )
            ),
            flow_status="Suspended",
            #trigger_config=appflow.CfnFlow.TriggerConfigProperty(
            #    trigger_type="OnDemand"
            #),
            tasks = app_flow_tasks
            
        )

        '''
        LOAD LAYER - Lambda & State Machine
        '''

        # Define a Lambda Layer for the psycopg2 library
        psycopg2_layer = lambda_.LayerVersion(
            self, f'Psycopg2Layer{salesforce_object}',
            code=lambda_.Code.from_asset('lambda_layers/API_Layer'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8, lambda_.Runtime.PYTHON_3_9],
            description="A layer for psycopg2",
        )

        # Define a Lambda to list all files
        lambda_list_s3_files = lambda_.Function(self, f"LambdaListFilesSalesforce{salesforce_object}",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name=f"ft-{env}-s3-list-salesforce-{entity_name}-files",
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(30),
            code=lambda_.Code.from_asset('lambdas/ListS3Files'),
            handler='lambda_function.lambda_handler',
            environment={
                "BUCKET_NAME": ds_base_stack.data_lake_bucket.bucket_name,
                "BUCKET_FOLDER": s3_bucket_folder
            }
        )

        # Grant the Lambda function permissions to read from the Data Lake bucket 
        ds_base_stack.data_lake_bucket.grant_read_write(lambda_list_s3_files)
        
        # Define a Lambda to Process all Files
        lambda_process_files = lambda_.Function(self, f"LambdaProcessLoadLayerFilesSalesforce{salesforce_object}",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name=f"ft-{env}-salesforce-{entity_name}-load-files",
            layers=[psycopg2_layer],
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.minutes(15),
            code=lambda_.Code.from_asset(f'lambdas/LoadLayer/Salesforce/{salesforce_object}'),
            handler='lambda_function.lambda_handler',
            environment={
                "BUCKET_NAME": ds_base_stack.data_lake_bucket.bucket_name,
                "BUCKET_FOLDER": s3_bucket_folder,
                "COMMIT_BATCH_SIZE": commit_batch_size
            }
        )
         # Grant the Lambda function permissions to read from and write to the S3 data lake
        ds_base_stack.data_lake_bucket.grant_read_write(lambda_process_files)

        # Create Tasks for State Machine

        # Step 1: Retrieve Secrets
        retrieve_secrets_task = tasks.LambdaInvoke(
            self, f"Retrieve Secrets for {salesforce_object}",
            lambda_function=ds_base_stack.lambda_retrieve_secrets,
            result_path="$.secret"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "RetrieveSecretsFailedLoadLayer", error="RetrieveSecretsFailed", cause="Failed to retrieve secrets"),
            errors=["States.ALL"]
        )

        
        #Step 2: List S3 Files
        list_s3_files_task = tasks.LambdaInvoke(
            self, f"List S3 Files available to ingest for {salesforce_object}",
            lambda_function=lambda_list_s3_files,
            result_path="$.files"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "ListS3FilesFailedLoadLayer", error="ListS3FilesFailed", cause="Failed to list files from S3"),
            errors=["States.ALL"]
        )
        

        # Step 3: Process Files in Batch
        process_files_task = sfn.Map(
            self, f"Process Files for {salesforce_object}",
            max_concurrency=int(concurrent_lambdas),
            items_path="$.files.Payload.files",  # list of files from previous step
            parameters={
                "file_name.$": "$$.Map.Item.Value",
                "secret.$": "$.secret.Payload.secret",  # Pass the secret to each iteration
                "timestamp.$": "$.timestamp.Payload.timestamp"  # Pass the timestamp to each iteration
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
            sfn.Fail(self, "ProcessFilesFailedLoadLayer", error="ProcessFilesFailed", cause="Failed to process files"),
            errors=["States.ALL"]
        )
        
         # Define the state machine workflow
        definition = retrieve_secrets_task.next(list_s3_files_task).next(process_files_task)

        # Create the state machine
        state_machine = sfn.StateMachine(
            self, f"FtLoadStateMachineSalesforce{salesforce_object}",
            state_machine_name=f"ft-{env}-load-layer-salesforce-{entity_name}",
            definition=definition
        )

        
        # Create an EventBridge rule to capture the AppFlow run complete event
        appflow_event_rule = events.Rule(
            self, f"AppFlowRunCompleteRuleSalesforce{salesforce_object}",
            rule_name=f"ft-{env}-ingestion-layer-salesforce-{entity_name}-success",
            event_pattern= events.EventPattern(
                source=["aws.appflow"],
                detail_type=["AppFlow End Flow Run Report"],
                detail={
                    "status": ["Execution Successful"],  
                    "flow-name": [self.ingestion_appflow.flow_name],
                    "num-of-records-processed": [{
                        "anything-but": ["0"]
                    }]
                }
            )
        ) 

        # Set the target of the rule to the Step Functions state machine
        appflow_event_rule.add_target(targets.SfnStateMachine(state_machine))

        '''
        TRANFORMATION LAYER
        '''
        # Create Tasks for State Machine

        # Step 1: Retrieve Secrets
        retrieve_secrets_task = tasks.LambdaInvoke(
            self, "Retrieve Secrets for Transform Layer",
            lambda_function=ds_base_stack.lambda_retrieve_secrets,
            result_path="$.secret"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "RetrieveSecretsFailed", error="RetrieveSecretsFailed", cause="Failed to retrieve secrets"),
            errors=["States.ALL"]
        )

        # Step 2: Call ft_ds_admin.raw_to_valid_sf_contact
        execute_raw_to_valid_task = tasks.LambdaInvoke(
            self, "Execute Raw to Valid",
            lambda_function=ds_base_stack.lambda_execute_stored_procedure,
            payload=sfn.TaskInput.from_object({
                "procedure_name": "ft_ds_admin.raw_to_valid_sf_contact",
                "secret": sfn.JsonPath.object_at("$.secret.Payload.secret")
            }),
            result_path="$.raw_to_valid_result"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "Stored Procedure raw_to_valid failed", error="Stored Procedure raw_to_valid failed", cause="Stored Procedure raw_to_valid failed"),
            errors=["States.ALL"]
        )

        # Step 3: Call ft_ds_admin.valid_to_refined_sf_contact
        # ft_ds_admin.historical_metric_sf_contact_counts_by_chapter
        execute_valid_to_refined_task = tasks.LambdaInvoke(
            self, "Execute Valid to Refined",
            lambda_function=ds_base_stack.lambda_execute_stored_procedure,
            payload=sfn.TaskInput.from_object({
                "procedure_name": "ft_ds_admin.valid_to_refined_sf_contact",
                "secret": sfn.JsonPath.object_at("$.secret.Payload.secret")
            }),
            result_path="$.valid_to_refined_result"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "Stored Procedure valid_to_refined failed", error="Stored Procedure valid_to_refined failed", cause="Stored Procedure valid_to_refined failed"),
            errors=["States.ALL"]
        )

        # Step 4: Call ft_ds_admin.historical_metric_sf_contact_counts_by_chapter
        execute_historical_metrics_task = tasks.LambdaInvoke(
            self, "Execute Historical Metrics",
            lambda_function=ds_base_stack.lambda_execute_stored_procedure,
            payload=sfn.TaskInput.from_object({
                "procedure_name": "ft_ds_admin.historical_metric_sf_contact_counts_by_chapter",
                "secret": sfn.JsonPath.object_at("$.secret.Payload.secret")
            }),
            result_path="$.historical_metrics_result"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "Stored Procedure historical_metrics failed", error="Stored Procedure historical_metrics failed", cause="Stored Procedure historical_metrics failed"),
            errors=["States.ALL"]
        )


         # Define the state machine workflow
        definition = retrieve_secrets_task.next(execute_raw_to_valid_task).next(execute_valid_to_refined_task).next(execute_historical_metrics_task)

        # Create the state machine
        state_machine = sfn.StateMachine(
            self, "FtTransformStateMachine",
            state_machine_name=f"ft-{env}-transform-layer-salesforce-{entity_name}",
            definition=definition,
            timeout=Duration.minutes(60)
        )

