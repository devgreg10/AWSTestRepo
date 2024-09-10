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

from constructs import Construct

from dotenv import load_dotenv
import os

# Global Variables
data_lake_bucket = None

class FtSalesforceContactIngestionLayerStack(Stack):

    def __init__(self, scope: Construct, id: str, env: str, datalake_bucket_folder: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        elif (env=='uat'):
            load_dotenv(os.path.join(BASEDIR,"../.env.uat"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.dev"))

         # Create S3 bucket
        self.data_lake_bucket = s3.Bucket(self, 
            "FTDevDataLakeBucket",
            bucket_name=f"ft-{env}-data-lake"
        )

        # Define bucket policy for AppFlow
        appflow_policy_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            principals=[iam.ServicePrincipal("appflow.amazonaws.com")],
            actions=[
                "s3:PutObject",
                "s3:GetBucketAcl",
                "s3:PutObjectAcl"
            ],
            resources=[
                self.data_lake_bucket.bucket_arn,  # Bucket itself
                f"{self.data_lake_bucket.bucket_arn}/*"  # All objects in the bucket
            ]
        )

        # Attach AppFlow policy to the bucket
        self.data_lake_bucket.add_to_resource_policy(appflow_policy_statement)

        # Define bucket policy for AWS Management Console users to move files
        move_files_policy_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            principals=[iam.AccountRootPrincipal()],  # Allows any user from the account
            actions=[
                "s3:ListBucket",    # List objects in the bucket
                "s3:GetObject",     # Get objects (needed for move)
                "s3:PutObject",     # Put objects (needed for move)
                "s3:DeleteObject"   # Delete objects (needed for move)
            ],
            resources=[
                self.data_lake_bucket.bucket_arn,        # Required for s3:ListBucket
                f"{self.data_lake_bucket.bucket_arn}/*"  # All objects in the bucket
            ]
        )

        # Attach the move files policy to the bucket
        self.data_lake_bucket.add_to_resource_policy(move_files_policy_statement)

        
        # AppFlow will be scheduled to start 15 min from now, so get the current time in UTC
        now = datetime.now(timezone.utc)

        # Add 15 minutes to the current time
        future_time = now + timedelta(minutes=15)

        # Convert the future time to a UNIX timestamp
        unix_timestamp = int(future_time.timestamp())
       
        # Create the AppFlow flow
        self.ingestion_appflow = appflow.CfnFlow(
            self, "SalesforceToS3Flow",
            flow_name=f"ft-{env}-ingestion-layer-salesforce-contact",
            source_flow_config=appflow.CfnFlow.SourceFlowConfigProperty(
                connector_type="Salesforce",
                connector_profile_name=os.getenv('salesforce_connection_name'),
                source_connector_properties=appflow.CfnFlow.SourceConnectorPropertiesProperty(
                    salesforce=appflow.CfnFlow.SalesforceSourcePropertiesProperty(
                        object="Contact",
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
                        bucket_name=self.data_lake_bucket.bucket_name,
                        bucket_prefix=f"{datalake_bucket_folder}ingress",
                        s3_output_format_config=appflow.CfnFlow.S3OutputFormatConfigProperty(
                            aggregation_config=appflow.CfnFlow.AggregationConfigProperty(
                                aggregation_type="None",
                                target_file_size=128
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
            tasks = [
                appflow.CfnFlow.TaskProperty(
                    source_fields=[
                        "Id", "Chapter_Affiliation__c", "ChapterID_CONTACT__c", "CASESAFEID__c",
                        "Contact_Type__c", "Age__c", "Ethnicity__c", "Gender__c", "Grade__c",
                        "Participation_Status__c", "MailingPostalCode", "MailingStreet",
                        "MailingCity", "MailingState", "School_Name__c", "School_Name_Other__c",
                        "FirstName", "LastName", "Birthdate", "AccountId", "LastModifiedDate",
                        "IsDeleted", "CreatedDate"
                    ],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="PROJECTION"
                    ),
                    task_type="Filter",
                    task_properties=[]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Id"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="Id",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="id"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="id"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Chapter_Affiliation__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="Chapter_Affiliation__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="reference"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="reference"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["ChapterID_CONTACT__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="ChapterID_CONTACT__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="string"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["CASESAFEID__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="CASESAFEID__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="string"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Contact_Type__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="Contact_Type__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="multipicklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="multipicklist"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Age__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="Age__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="double"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="double"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Ethnicity__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="Ethnicity__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="picklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="picklist"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Gender__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="Gender__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="picklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="picklist"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Grade__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="Grade__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="picklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="picklist"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Participation_Status__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="Participation_Status__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="picklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="picklist"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["MailingPostalCode"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="MailingPostalCode",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="string"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["MailingStreet"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="MailingStreet",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="textarea"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="textarea"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["MailingCity"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="MailingCity",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="string"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["MailingState"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="MailingState",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="string"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["School_Name__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="School_Name__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="string"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["School_Name_Other__c"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="School_Name_Other__c",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="string"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["FirstName"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="FirstName",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="string"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["LastName"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="LastName",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="string"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Birthdate"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="Birthdate",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="date"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="date"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["AccountId"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="AccountId",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="reference"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="reference"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["LastModifiedDate"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="LastModifiedDate",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="datetime"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="datetime"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["IsDeleted"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="IsDeleted",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="boolean"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="boolean"
                        ),
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["CreatedDate"],
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    destination_field="CreatedDate",
                    task_type="Map",
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE",
                            value="datetime"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE",
                            value="datetime"
                        ),
                    ]
                ),
            ]
        )
