from aws_cdk import (
    SecretValue,
    Stack,
    RemovalPolicy,
    aws_appflow as appflow,
    aws_secretsmanager as secretsmanager,
    aws_s3 as s3,
    aws_iam as iam
)

from datetime import datetime, timedelta
import pytz

from cloud.layer_4_ingestion_layer.appflow.tasks.ft_salesforce_account_tasks import FtSalesforceAccountAppFlowTasks
from cloud.layer_2_storage.ft_decision_support_persistent_storage_stack import FtDecisionSupportPersistentStorageStack
from cloud.layer_4_ingestion_layer.appflow.tasks.ft_salesforce_contact_tasks import FtSalesforceContactAppFlowTasks
from cloud.layer_4_ingestion_layer.appflow.tasks.ft_salesforce_listing_session_tasks import FtSalesforceListingSessionAppFlowTasks
from cloud.layer_4_ingestion_layer.appflow.tasks.ft_salesforce_listing_tasks import FtSalesforceListingAppFlowTasks
from cloud.layer_4_ingestion_layer.appflow.tasks.ft_salesforce_session_registration_tasks import FtSalesforceSessionRegistrationAppFlowTasks
from cloud.layer_4_ingestion_layer.appflow.tasks.ft_salesforce_badge_tasks import FtSalesforceBadgeAppFlowTasks
from cloud.layer_4_ingestion_layer.appflow.tasks.ft_salesforce_earned_badges_tasks import FtSalesforceEarnedBadgesAppFlowTasks

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
                 storage_stack: FtDecisionSupportPersistentStorageStack, **kwargs) -> None:
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
               "entity_name": "account", 
                "salesforce_object": "Account", 
                "appflow_tasks": FtSalesforceAccountAppFlowTasks(self, 
                                                                 "SaleforceAccountTasks",
                                                                 operates_the_facility_through_a_lease_field_name=os.getenv('operates_the_facility_through_a_lease_field_name'),
                                                                 dedicated_first_tee_learning_center_field_name=os.getenv('dedicated_first_tee_learning_center_field_name'),
                                                                 discounts_offered_to_participants_field_name=os.getenv('discounts_offered_to_participants_field_name'))
            }, 
            {
               "entity_name": "badge", 
                "salesforce_object": "Gamification_Badge__c", 
                "appflow_tasks": FtSalesforceBadgeAppFlowTasks(self, "SaleforceBadgeTasks")
            },             {
                "entity_name": "contact", 
                "salesforce_object": "Contact", 
                "appflow_tasks": FtSalesforceContactAppFlowTasks(self, "SaleforceContactTasks")
            },
            # {
            #     "entity_name": "earned-badge", 
            #     "salesforce_object": "Earned_Badge__c", 
            #     "appflow_tasks": FtSalesforceEarnedBadgesAppFlowTasks(self, "SaleforceEarnedBadgesTasks") 
            # },
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

            # Get the current time in New York time zone (EST/EDT)
            now = datetime.now(pytz.timezone("America/New_York"))

            # Define 9 PM (21:00) for today
            next_nine_pm = now.replace(hour=21, minute=0, second=0, microsecond=0)

            # If it's already past 9 PM, calculate for the next day
            if now >= next_nine_pm:
                next_nine_pm += timedelta(days=1)

            # Convert the time to a Unix timestamp (seconds since epoch)
            unix_timestamp = int(next_nine_pm.timestamp())
        
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
                            bucket_name=storage_stack.data_lake_bucket.bucket_name,
                            bucket_prefix=f"{s3_bucket_folder}ingress",
                            s3_output_format_config=appflow.CfnFlow.S3OutputFormatConfigProperty(
                                aggregation_config=appflow.CfnFlow.AggregationConfigProperty(
                                    aggregation_type="None",
                                    target_file_size=16 # 16MB target file size
                                ),
                                file_type="JSON",
                                prefix_config=appflow.CfnFlow.PrefixConfigProperty(
                                    prefix_format="MINUTE", # Determines the level of granularity for the date and time that's included in the prefix.
                                    prefix_type="FILENAME", # Determines the format of the prefix, and whether it applies to the file name, file path, or both.
                                    path_prefix_hierarchy=[
                                        "SCHEMA_VERSION",
                                        "EXECUTION_ID"
                                    ]
                                ),
                                preserve_source_data_typing=False # all source data converted into strings
                            )
                        )
                    )
                )],
                trigger_config=appflow.CfnFlow.TriggerConfigProperty(
                    trigger_type="Scheduled",
                    trigger_properties=appflow.CfnFlow.ScheduledTriggerPropertiesProperty(
                        schedule_expression="rate(12hours)",
                        data_pull_mode="Incremental",
                        schedule_start_time=unix_timestamp,
                        time_zone="America/New_York",
                        schedule_offset=0
                    )
                ),
                flow_status="Active",
                # trigger_config=appflow.CfnFlow.TriggerConfigProperty(
                #    trigger_type="OnDemand"
                # ),
                tasks = appflow_tasks
                
            )


