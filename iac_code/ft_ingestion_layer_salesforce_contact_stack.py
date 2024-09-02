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

    def __init__(self, scope: Construct, id: str, env: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.non_prod"))

        datalake_bucket_name = "ft-" + env + "-data-lake"
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


        # Reference the existing Salesforce connection
        salesforce_connection = appflow.CfnConnectorProfile(
            self, "SalesforceConnectorProfile",
            connector_profile_name=os.getenv('salesforce_connection_name'),
            connection_mode="PUBLIC",
            connector_type="Salesforce"
        )

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
            destination_flow_config_list=[appflow.CfnFlow.DestinationFlowConfigProperty(
                connector_type="S3",
                destination_connector_properties=appflow.CfnFlow.DestinationConnectorPropertiesProperty(
                    s3=appflow.CfnFlow.S3DestinationPropertiesProperty(
                        bucket_name=destination_bucket.bucket_name,
                        bucket_prefix="salesforce/ingress/contact/snapshot/",
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
            source_flow_config=appflow.CfnFlow.SourceFlowConfigProperty(
                connector_type="Salesforce",
                source_connector_properties=appflow.CfnFlow.SourceConnectorPropertiesProperty(
                    salesforce=appflow.CfnFlow.SalesforceSourcePropertiesProperty(
                        object="Contact",
                        enable_dynamic_field_update=False,
                        include_deleted_records=False
                    )
                ),
                connector_profile_name=salesforce_connection.connector_profile_name
            ),
            tasks=[
                appflow.CfnFlow.TaskProperty(
                    source_fields=[
                        'Id', 
                        'MailingPostalCode', 
                        'Chapter_Affiliation__c', 
                        'ChapterID_CONTACT__c', 
                        'CASESAFEID__c', 
                        'Contact_Type__c', 
                        'Age__c', 
                        'Ethnicity__c', 
                        'Gender__c', 
                        'Grade__c', 
                        'Participation_Status__c'
                    ],
                    task_type="Map_all",
                    destination_field="S3"
                )
            ],
            trigger_config=appflow.CfnFlow.TriggerConfigProperty(
                trigger_type="OnDemand"
            )
        )
