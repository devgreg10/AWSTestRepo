import os

import aws_cdk as cdk

from iac_code.layer_1.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack
from iac_code.layer_2.ft_decision_support_persistent_storage_stack import FtDecisionSupportPersistentStorageStack
from iac_code.ft_decision_support_base_stack import FtDecisionSupportBaseStack
from iac_code.ft_ingestion_layer_salesforce_stack import FtIngestionLayerSalesforceStack
from iac_code.ft_load_layer_salesforce_stack import FtLoadLayerSalesforceStack
from iac_code.ft_transform_layer_salesforce_stack import FtTransformLayerSalesforceStack

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
BOOTSTRAP LAYER
'''

decision_support_bootstrap_stack = FtDecisionSupportBootstrapStack(
    app,
    id=f"ft-1-{env}-decision-support-bootstrap-stack",
    env = env
)

'''
NETWORK LAYER
'''

'''
PERSISTENT STORAGE LAYER
'''
decision_support_persistent_storage_stack = FtDecisionSupportPersistentStorageStack(
    app, 
    id = f"ft-2-{env}-decision-support-persistent-storage-stack",
    env = env,
    boostrap_stack = decision_support_bootstrap_stack
)

# decision_support_base_stack = FtDecisionSupportBaseStack(
#     app, 
#     id=f"ft-{env}-decision-support-base-stack", 
#     env=env, 
#     secret_region=os.getenv('default_region'),
#     version_number=os.getenv('version_number')
# )

# '''
# INGESTION LAYER
# '''

# #Salesforce
# ingestion_layer_salesforce_stack = FtIngestionLayerSalesforceStack(
#     app, 
#     id = f"ft-{env}-ingestion-layer-salesforce-stack",
#     env = env,
#     ds_base_stack = decision_support_base_stack
# )

# '''
# LOAD LAYER
# '''

# #Salesforce
# load_layer_salesforce_stack = FtLoadLayerSalesforceStack(
#     app, 
#     id = f"ft-{env}-load-layer-salesforce-stack",
#     env = env,
#     region=os.getenv('default_region'),
#     email_addresses_to_alert_on_error=os.getenv('email_addresses_to_alert_on_error'),
#     concurrent_lambdas = os.getenv('load_layer_salesforce_concurrent_lambdas'),
#     commit_batch_size = os.getenv('load_layer_commit_batch_size'),
#     ds_base_stack = decision_support_base_stack,
#     ingestion_layer_stack=ingestion_layer_salesforce_stack
# )

# '''
# TRANSFORM LAYER
# '''

# #Salesforce
# transform_layer_salesforce_stack = FtTransformLayerSalesforceStack(
#     app, 
#     id = f"ft-{env}-transform-layer-salesforce-stack",
#     env = env,
#     region=os.getenv('default_region'),
#     ds_base_stack = decision_support_base_stack,
#     ingestion_layer_stack=ingestion_layer_salesforce_stack,
#     load_layer_stack=load_layer_salesforce_stack
# )

'''
TAGS
'''
cdk.Tags.of(app).add("Project", "First Tee Decision Support")
cdk.Tags.of(app).add("Deployment", "CDK")
cdk.Tags.of(app).add("Repo", "FT-decision-support-cloud")

app.synth()