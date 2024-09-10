import os

import aws_cdk as cdk

from iac_code.ft_create_secret import FtCreateSecretsStack
from iac_code.ft_load_layer_salesforce_contact_stack import FtLoadLayerSalesforceContactStack
from iac_code.ft_transform_layer_salesforce_contact_stack import FtTransformLayerSalesforceContactStack
from iac_code.ft_ingestion_layer_salesforce_contact_stack import FtSalesforceContactIngestionLayerStack

from dotenv import load_dotenv

load_dotenv()
env = os.environ.get('env')

app = cdk.App()

if (env=='prod'):
    load_dotenv(".env.prod")
elif(env=='uat'):
    load_dotenv(".env.uat")
else:
    load_dotenv(".env.dev")

'''
ZZZ - Temporary Secret for DB
'''
create_secret_stack = FtCreateSecretsStack(app, f"ft-{env}-create-db-secret")

'''
BEGIN - Salesforce Contact Entity
'''
    
ingestion_layer_salesforce_contact_stack = FtSalesforceContactIngestionLayerStack(app, f"ft-{env}-ingestion-layer-salesforce-contact-stack", 
    env=env,
    datalake_bucket_folder=os.getenv('load_layer_salesforce_contact_s3_bucket_folder'))

load_layer_salesforce_contact_stack = FtLoadLayerSalesforceContactStack(app, f"ft-{env}-load-layer-salesforce-contact-stack",
    env=env,
    bucket_folder=os.getenv('load_layer_salesforce_contact_s3_bucket_folder'),
    concurrent_lambdas=os.getenv('load_layer_salesforce_concurrent_lambda'),
    commit_batch_size=os.getenv('load_layer_commit_batch_size'),
    secret_region=os.getenv('db_connection_secret_region'),
    secret_layer_stack=create_secret_stack,
    ingestion_layer_stack=ingestion_layer_salesforce_contact_stack
)

tranform_layer_salesforce_contact_stack = FtTransformLayerSalesforceContactStack(app, f"ft-{env}-tranform-layer-salesforce-contact-stack",
    env=env,
    load_layer_stack=load_layer_salesforce_contact_stack
)

'''
END - Salesforce Contact Entity
'''

cdk.Tags.of(app).add("Project", "First Tee Decision Support")
cdk.Tags.of(app).add("Deployment", "CDK")
cdk.Tags.of(app).add("Repo", "FT-decision-support-cloud")

app.synth()