#!/usr/bin/env python3
import os

import aws_cdk as cdk

from iac_code.first_tee_decision_support_stack import FirstTeeDecisionSupportStack
from iac_code.ft_appflow_s3_to_s3_ingestion import FtAppflowS3ToS3IngestionStack
from iac_code.ft_data_warehouse_aurora import FtDataWarehouseAuroraStack
from iac_code.ft_ec2_key_pair_lambda import FtEc2KeyPairLambda

from dotenv import load_dotenv

load_dotenv()
env = os.environ.get('env')

app = cdk.App()

# FirstTeeDecisionSupportStack(app, "FirstTee-CDK-{0}".format(env), env)
# FtAppflowS3ToS3IngestionStack(app, "FirstTee-CDK-AppFlow-S3-To-S3-Ingestion-{0}".format(env), env)
# FtDataWarehouseAuroraStack(app, "FirstTee-CDK-Data-Warehouse-Aurora-{0}".format(env), env)
FtEc2KeyPairLambda(app, "FirstTee-CDK-Ec2-Key-Pair-Lambda", env)

cdk.Tags.of(app).add("Project", "First Tee Decision Support")
cdk.Tags.of(app).add("Deployment", "CDK")
cdk.Tags.of(app).add("Repo", "First_Tee_Decision_Support")

app.synth()