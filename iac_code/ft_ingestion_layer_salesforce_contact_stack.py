from aws_cdk import (
    SecretValue,
    Stack,
    RemovalPolicy,
    aws_appflow as appflow,
    aws_secretsmanager as secretsmanager,
    aws_s3 as s3,
    aws_iam as iam
)
from constructs import Construct

from dotenv import load_dotenv
import os

class FtSalesforceContactIngestionLayerStack(Stack):

    def __init__(self, scope: Construct, id: str, env: str, datalake_bucket_name: str, datalake_bucket_folder: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.non_prod"))

        # Destination S3 bucket
        try:
            destination_bucket = s3.Bucket.from_bucket_name(self, "ExistingBucket", datalake_bucket_name)
            print(f"Bucket {datalake_bucket_name} exists.")
        except Exception:
            print(f"Bucket {datalake_bucket_name} does not exist. Creating a new bucket.")
            destination_bucket = s3.Bucket(self, "DestinationBucket",
                                       bucket_name=datalake_bucket_name, 
                                       versioned=True,
                                       removal_policy=RemovalPolicy.DESTROY)

        # IAM role for the AppFlow
        appflow_role = iam.Role(
            self, "AppFlowServiceRole",
            assumed_by=iam.ServicePrincipal("appflow.amazonaws.com")
        )

        # Attach the necessary policies to the role
        destination_bucket.grant_read_write(appflow_role)

        # Create the AppFlow flow
        flow = appflow.CfnFlow(
            self, "SalesforceToS3Flow",
            flow_name="ft-" + env + "-ingestion-layer-salesforce-contact",
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
                        bucket_name=destination_bucket.bucket_name,
                        bucket_prefix="salesforce/contact/ingress",
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
                    schedule_expression="rate(1 days)",
                    data_pull_mode="Incremental",
                    schedule_start_time=1725436800, # https://www.unixtimestamp.com/
                    time_zone="America/New_York",
                    schedule_offset=0
                )
            ),
            flow_status="Active",
            tasks=[
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Participation_Status__c"],
                    task_type="Filter",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NOT_EQUAL_TO"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DATA_TYPE", value="picklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="VALUE", value="Previous"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=[
                        "Id", "Chapter_Affiliation__c", "ChapterID_CONTACT__c",
                        "CASESAFEID__c", "Contact_Type__c", "Age__c", 
                        "Ethnicity__c", "Gender__c", "Grade__c", 
                        "Participation_Status__c", "MailingPostalCode", 
                        "MailingStreet", "MailingCity", "MailingState", 
                        "School_Name__c", "School_Name_Other__c", 
                        "FirstName", "LastName", "Birthdate", "AccountId"
                    ],
                    task_type="Filter",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="PROJECTION"
                    ),
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Id"],
                    task_type="Map",
                    destination_field="Id",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="id"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="id"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Chapter_Affiliation__c"],
                    task_type="Map",
                    destination_field="Chapter_Affiliation__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="reference"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="reference"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["ChapterID_CONTACT__c"],
                    task_type="Map",
                    destination_field="ChapterID_CONTACT__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="string"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["CASESAFEID__c"],
                    task_type="Map",
                    destination_field="CASESAFEID__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="string"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Contact_Type__c"],
                    task_type="Map",
                    destination_field="Contact_Type__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="multipicklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="multipicklist"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Age__c"],
                    task_type="Map",
                    destination_field="Age__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="double"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="double"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Ethnicity__c"],
                    task_type="Map",
                    destination_field="Ethnicity__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="picklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="picklist"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Gender__c"],
                    task_type="Map",
                    destination_field="Gender__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="picklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="picklist"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Grade__c"],
                    task_type="Map",
                    destination_field="Grade__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="picklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="picklist"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Participation_Status__c"],
                    task_type="Map",
                    destination_field="Participation_Status__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="picklist"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="picklist"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["MailingPostalCode"],
                    task_type="Map",
                    destination_field="MailingPostalCode",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="string"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["MailingStreet"],
                    task_type="Map",
                    destination_field="MailingStreet",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="textarea"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="textarea"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["MailingCity"],
                    task_type="Map",
                    destination_field="MailingCity",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="string"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["MailingState"],
                    task_type="Map",
                    destination_field="MailingState",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="string"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["School_Name__c"],
                    task_type="Map",
                    destination_field="School_Name__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="string"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["School_Name_Other__c"],
                    task_type="Map",
                    destination_field="School_Name_Other__c",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="string"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["FirstName"],
                    task_type="Map",
                    destination_field="FirstName",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="string"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["LastName"],
                    task_type="Map",
                    destination_field="LastName",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="string"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="string"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["Birthdate"],
                    task_type="Map",
                    destination_field="Birthdate",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="date"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="date"
                        )
                    ]
                ),
                appflow.CfnFlow.TaskProperty(
                    source_fields=["AccountId"],
                    task_type="Map",
                    destination_field="AccountId",
                    connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                        salesforce="NO_OP"
                    ),
                    task_properties=[
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="DESTINATION_DATA_TYPE", value="reference"
                        ),
                        appflow.CfnFlow.TaskPropertiesObjectProperty(
                            key="SOURCE_DATA_TYPE", value="reference"
                        )
                    ]
                ),
            ]
        )
