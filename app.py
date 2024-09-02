#!/usr/bin/env python3
import os

import aws_cdk as cdk

from iac_code.first_tee_decision_support_stack import FirstTeeDecisionSupportStack
from iac_code.ft_appflow_s3_to_s3_ingestion import FtAppflowS3ToS3IngestionStack
from iac_code.ft_data_warehouse_aurora import FtDataWarehouseAuroraStack
from iac_code.ft_ec2_key_pair_lambda import FtEc2KeyPairLambda
from iac_code.ft_public_data_warehouse_aurora import FtPublicDataWarehouseAuroraStack
from iac_code.ft_create_secret import FtCreateSecretsStack
from iac_code.ft_s3_to_aurora_load import FtS3ToAuroraLoadStack
from iac_code.ft_load_layer_salesforce_stack import FtLoadLayerSalesforceStack
from iac_code.ft_transform_layer_salesforce_stack import FtTransformLayerSalesforceStack
from iac_code.ft_ingestion_layer_salesforce_contact_stack import FtSalesforceContactIngestionLayerStack

from dotenv import load_dotenv

load_dotenv()
env = os.environ.get('env')

app = cdk.App()

if (env=='prod'):
    load_dotenv(".env.prod")
elif (env=='uat'):
    load_dotenv(".env.uat")
else:
    load_dotenv(".env.dev")

# FirstTeeDecisionSupportStack(app, "FirstTee-CDK-{0}".format(env), env)
# FtAppflowS3ToS3IngestionStack(app, "FirstTee-CDK-AppFlow-S3-To-S3-Ingestion-{0}".format(env), env)
# FtDataWarehouseAuroraStack(app, "FirstTee-CDK-Data-Warehouse-Aurora-{0}".format(env), env)
# FtEc2KeyPairLambda(app, "FirstTee-CDK-Ec2-Key-Pair-Lambda", env)
# FtPublicDataWarehouseAuroraStack(app, "FirstTee-CDK-Public-Data-Warehouse-Aurora-{0}".format(env), env)
# FtCreateSecretsStack(app, "AuroraSecretsStack")
# FtS3ToAuroraLoadStack(app, "FtS3ToAuroraLoadStack",
    
FtSalesforceContactIngestionLayerStack(app, "ft-" + env + "-ingestion-layer-salesforce-contact-stack", env)

'''
FtLoadLayerSalesforceStack(app, "FtLoadLayerSalesforceStack",
    env=env,
    secret_arn=os.getenv('db_connection_secret_arn'),
    secret_region=os.getenv('db_connection_secret_region'),
    bucket_name=os.getenv('load_layer_s3_bucket_name'),
    bucket_folder=os.getenv('load_layer_salesforce_s3_bucket_folder'),
    file_batch_size=os.getenv('load_layer_salesforce_file_batch_size'),
    concurrent_lambdas=os.getenv('load_layer_salesforce_concurrent_lambda')
)


FtTransformLayerSalesforceStack(app, "FtTransformLayerSalesforceStack",
    env=env,
    secret_arn=os.getenv('db_connection_secret_arn'),
    secret_region=os.getenv('db_connection_secret_region')
)

'''

cdk.Tags.of(app).add("Project", "First Tee Decision Support")
cdk.Tags.of(app).add("Deployment", "CDK")
cdk.Tags.of(app).add("Repo", "First_Tee_Decision_Support")

app.synth()