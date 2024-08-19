from aws_cdk import (
    aws_s3 as s3,
    aws_appflow as appflow,
    aws_iam as iam,
    aws_s3_deployment as s3deploy,
    Stack,
    RemovalPolicy
)

from constructs import Construct
from dotenv import load_dotenv
import os

load_dotenv()

class FtAppflowS3ToS3IngestionStack(Stack):

   def __init__(self, scope: Construct, construct_id: str, env: str, **kwargs) -> None:
        if (env == None):
            env = 'no_env'

        super().__init__(scope, construct_id, **kwargs)

        # Source S3 bucket
        source_bucket = s3.Bucket(self, "SourceBucket",
                                  bucket_name="ft-" + env + "-ingestion-source-bucket", #source-bucket-name (ft-ingestion-source-bucket)
                                  versioned=True,
                                  removal_policy=RemovalPolicy.DESTROY)
        
         # Seed file needed for AppFlow to map column headers
        s3deploy.BucketDeployment(self, "DeployInitialCSV",
                                  sources=[s3deploy.Source.asset("seed_files/")],
                                  destination_bucket=source_bucket,
                                  destination_key_prefix="ingestion-source/")

        # Destination S3 bucket
        destination_bucket = s3.Bucket(self, "DestinationBucket",
                                       bucket_name="ft-" + env + "-data-lake", 
                                       #destination-bucket-name (ft-ingestion-destination-bucket)
                                       versioned=True,
                                       removal_policy=RemovalPolicy.DESTROY)
        # AppFlow role
        appflow_role = iam.Role(self, "ft-" + env + "-ingestion-appflow-role",
            assumed_by=iam.ServicePrincipal("appflow.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
            ]
        )

        # Explicit permissions for AppFlow
        source_bucket_policy_statement = iam.PolicyStatement(
            actions=["s3:ListBucket", "s3:GetObject"],
            resources=[source_bucket.bucket_arn, source_bucket.arn_for_objects("*")],
            principals=[iam.ServicePrincipal("appflow.amazonaws.com")]
        )
        source_bucket.add_to_resource_policy(source_bucket_policy_statement)

        # Grant necessary permissions to AppFlow service principal
        source_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("appflow.amazonaws.com")],
                actions=["s3:ListBucket", "s3:GetObject"],
                resources=[source_bucket.bucket_arn, f"{source_bucket.bucket_arn}/*"]
            )
        )

        destination_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("appflow.amazonaws.com")],
                actions=["s3:PutObject", "s3:GetBucketAcl", "s3:PutObjectAcl"],
                resources=[destination_bucket.bucket_arn, f"{destination_bucket.bucket_arn}/*"]
            )
        )

        # Define a mapping task
        mapping_task = appflow.CfnFlow.TaskProperty(
            source_fields=[], 
            task_type="Map_all"
        )

        # Define a filter task
        filter_task = appflow.CfnFlow.TaskProperty(
            task_type="Filter",
            source_fields=["CASESAFEID__C","BIRTHDATE","AGE__C","GENDER__C","ETHNICITY__C","MAILINGPOSTALCODE","PARTICIPATION_STATUS__C","CONTACT_TYPE__C","CHAPTER_AFFILIATION__C"], 
            connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(s3="PROJECTION")
        )

        # AppFlow source configuration
        source_config = appflow.CfnFlow.SourceFlowConfigProperty(
            connector_type="S3",
            source_connector_properties=appflow.CfnFlow.SourceConnectorPropertiesProperty(
                s3=appflow.CfnFlow.S3SourcePropertiesProperty(
                    bucket_name=source_bucket.bucket_name,
                    bucket_prefix="ingestion-source/"
                )
            )
        )

        # AppFlow destination configuration with custom file naming
        destination_config = appflow.CfnFlow.DestinationFlowConfigProperty(
            connector_type="S3",
            destination_connector_properties=appflow.CfnFlow.DestinationConnectorPropertiesProperty(
                s3=appflow.CfnFlow.S3DestinationPropertiesProperty(
                    bucket_name=destination_bucket.bucket_name,
                    # bucket_prefix="ingress/",  
                    s3_output_format_config=appflow.CfnFlow.S3OutputFormatConfigProperty(
                        file_type="CSV",  # or JSON, CSV, Parquet etc.
                        prefix_config=appflow.CfnFlow.PrefixConfigProperty(
                            prefix_type="FILENAME",  # You can use 'FILENAME' or 'PATH'
                            prefix_format="MINUTE",  # Valid Values: YEAR | MONTH | DAY | HOUR | MINUTE (https://docs.aws.amazon.com/appflow/1.0/APIReference/API_PrefixConfig.html)
                        )
                    )
                )
            )
        )
        
        # AppFlow Flow
        flow = appflow.CfnFlow(
            self, "AppFlowIngestFlow",
            flow_name = "ft-" + env + "-s3-to-s3-ingestion-flow",
            source_flow_config = source_config,
            destination_flow_config_list = [destination_config],
            trigger_config = appflow.CfnFlow.TriggerConfigProperty(trigger_type="OnDemand"),
            tasks=[mapping_task,filter_task]
        )

        # Grant AppFlow permissions to access the S3 buckets
        source_bucket.grant_read_write(appflow_role)
        destination_bucket.grant_read_write(appflow_role)
        

