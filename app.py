import os

import aws_cdk as cdk

from iac_code.ft_load_layer_salesforce_contact_stack import FtLoadLayerSalesforceContactStack
from iac_code.ft_transform_layer_salesforce_contact_stack import FtTransformLayerSalesforceContactStack
from iac_code.ft_ingestion_layer_salesforce_contact_stack import FtSalesforceContactIngestionLayerStack

from dotenv import load_dotenv

load_dotenv()
env = os.environ.get('env')

app = cdk.App()

if (env=='prod'):
    load_dotenv(".env.prod")
else:
    load_dotenv(".env.non-prod")
    if (env=='uat'):
        load_dotenv(".env.uat")
    else:
        load_dotenv(".env.dev")

'''
BEGIN - Salesforce Contact Entity
'''
    
ingestion_layer_salesforce_contact_stack = FtSalesforceContactIngestionLayerStack(app, "ft-" + env + "-ingestion-layer-salesforce-contact-stack", env)

load_layer_salesforce_contact_stack = FtLoadLayerSalesforceContactStack(app, "ft-" + env + "-load-layer-salesforce-contact-stack",
    env=env,
    secret_arn=os.getenv('db_connection_secret_arn'),
    secret_region=os.getenv('db_connection_secret_region'),
    bucket_name=os.getenv('load_layer_s3_bucket_name'),
    bucket_folder=os.getenv('load_layer_salesforce_s3_bucket_folder'),
    file_batch_size=os.getenv('load_layer_salesforce_file_batch_size'),
    concurrent_lambdas=os.getenv('load_layer_salesforce_concurrent_lambda'),
    commit_interval=os.getenv('load_layer_commit_interval')
)

tranform_layer_salesforce_contact_stack = FtTransformLayerSalesforceContactStack(app, "ft-" + env + "-tranform-layer-salesforce-contact-stack",
    env=env,
    secret_arn=os.getenv('db_connection_secret_arn'),
    secret_region=os.getenv('db_connection_secret_region')
)

load_layer_salesforce_contact_stack.add_dependency(ingestion_layer_salesforce_contact_stack)
tranform_layer_salesforce_contact_stack.add_dependency(load_layer_salesforce_contact_stack)

'''
END - Salesforce Contact Entity
'''

cdk.Tags.of(app).add("Project", "First Tee Decision Support")
cdk.Tags.of(app).add("Deployment", "CDK")
cdk.Tags.of(app).add("Repo", "FT-decision-support-cloud")

app.synth()