from aws_cdk import (
    SecretValue,
    Stack,
    RemovalPolicy,
    aws_appflow as appflow,
    aws_secretsmanager as secretsmanager,
    aws_s3 as s3,
    aws_iam as iam
)

from datetime import datetime, timedelta, timezone

from iac_code.layer_3.ft_decision_support_core_stack import FtDecisionSupportCoreStack
from iac_code.appflow.tasks.ft_salesforce_contact_tasks import FtSalesforceContactAppFlowTasks
from iac_code.appflow.tasks.ft_salesforce_listing_session_tasks import FtSalesforceListingSessionAppFlowTasks
from iac_code.appflow.tasks.ft_salesforce_listing_tasks import FtSalesforceListingAppFlowTasks
from iac_code.appflow.tasks.ft_salesforce_session_registration_tasks import FtSalesforceSessionRegistrationAppFlowTasks

from constructs import Construct

from dotenv import load_dotenv
import os

# Global Variables
data_lake_bucket = None

class FtIngestionLayerSalesforceStack(Stack):

    def __init__(self, 
                 scope: Construct, 
                 id: str, 
                 env: str, 
                 ds_core_stack: FtDecisionSupportCoreStack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        elif (env=='uat'):
            load_dotenv(os.path.join(BASEDIR,"../.env.uat"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.dev"))

        # Define all Salesforce entities and objects to ingest
        self.salesforce_entities = [
            {
                "entity_name": "contact", 
                "salesforce_object": "Contact", 
                "appflow_tasks": FtSalesforceContactAppFlowTasks(self, "SaleforceContactTasks")
            },
            {
                "entity_name": "listing", 
                "salesforce_object": "Listing__c",
                "appflow_tasks": FtSalesforceListingAppFlowTasks(self, "SaleforceListingTasks")
            },
            {
                "entity_name": "listing-session", 
                "salesforce_object": "Listing_Session__c",
                "appflow_tasks": FtSalesforceListingSessionAppFlowTasks(self, "SaleforceListingSessionTasks")
            },
            {
                "entity_name": "session-registration", 
                "salesforce_object": "Session_Registration__c",
                "appflow_tasks": FtSalesforceSessionRegistrationAppFlowTasks(self, "SaleforceSessionRegistrationTasks")
            }
        ]
    
        '''
        INGESTION LAYER - AppFlow
        Loop through salesforce_entities
        '''

        for salesforce_entity in self.salesforce_entities:

            entity_name = salesforce_entity["entity_name"]
            salesforce_object = salesforce_entity["salesforce_object"]
            appflow_tasks = salesforce_entity["appflow_tasks"].get_tasks()

            # S3 Bucket folder where this entity will be written
            s3_bucket_folder = f"salesforce/{entity_name}/"

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
                        datetime_type_field_name="SystemModstamp"
                    )
                ),
                destination_flow_config_list=[appflow.CfnFlow.DestinationFlowConfigProperty(
                    connector_type="S3",
                    destination_connector_properties=appflow.CfnFlow.DestinationConnectorPropertiesProperty(
                        s3=appflow.CfnFlow.S3DestinationPropertiesProperty(
                            bucket_name=ds_core_stack.data_lake_bucket.bucket_name,
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
                tasks = appflow_tasks
                
            )


