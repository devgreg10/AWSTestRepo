import os

from aws_cdk import App,Tags

from cloud.layer_1_bootstrap.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack
from cloud.layer_2_storage.ft_decision_support_persistent_storage_stack import FtDecisionSupportPersistentStorageStack
from cloud.layer_3_core.ft_decision_support_core_stack import FtDecisionSupportCoreStack
from cloud.layer_4_ingestion_layer.ft_ingestion_layer_salesforce_stack import FtIngestionLayerSalesforceStack
from cloud.layer_5_load_layer.ft_load_layer_salesforce_stack import FtLoadLayerSalesforceStack
from cloud.layer_6_transform_layer.ft_transform_layer_salesforce_stack import FtTransformLayerSalesforceStack

from dotenv import load_dotenv

load_dotenv()
env = os.environ.get('env')

app = App()

if (env=='prod'):
    load_dotenv(".env.prod")
elif(env=='uat'):
    load_dotenv(".env.uat")
else:
    load_dotenv(".env.dev")

'''
Layer 1 - BOOTSTRAP LAYER
'''
decision_support_bootstrap_stack = FtDecisionSupportBootstrapStack(
    app,
    id=f"ft-{env}-decision-support-bootstrap-stack",
    env = env
)

'''
Layer 2 - PERSISTENT STORAGE LAYER
'''
decision_support_persistent_storage_stack = FtDecisionSupportPersistentStorageStack(
    app, 
    id = f"ft-{env}-decision-support-persistent-storage-stack",
    env = env,
    email_addresses_to_alert_on_error=os.getenv('email_addresses_to_alert_on_error'),
    boostrap_stack = decision_support_bootstrap_stack
)

'''
Layer 3 - DECISION SUPPORT CORE LAYER
'''
decision_support_base_stack = FtDecisionSupportCoreStack(
    app, 
    id=f"ft-{env}-decision-support-base-stack", 
    env=env, 
    version_number=os.getenv('version_number'),
    storage_stack=decision_support_persistent_storage_stack,
)

'''
Layer 4 - INGESTION LAYER
'''

#Salesforce
ingestion_layer_salesforce_stack = FtIngestionLayerSalesforceStack(
    app, 
    id = f"ft-{env}-ingestion-layer-salesforce-stack",
    env = env,
    storage_stack = decision_support_persistent_storage_stack
)

'''
Layer 5 - LOAD LAYER
'''

#Salesforce
load_layer_salesforce_stack = FtLoadLayerSalesforceStack(
    app, 
    id = f"ft-{env}-load-layer-salesforce-stack",
    env = env,
    region=os.getenv('default_region'),
    email_addresses_to_alert_on_error=os.getenv('email_addresses_to_alert_on_error'),
    concurrent_lambdas = os.getenv('load_layer_salesforce_concurrent_lambdas'),
    commit_batch_size = os.getenv('load_layer_commit_batch_size'),
    bootstrap_stack = decision_support_bootstrap_stack,
    storage_stack = decision_support_persistent_storage_stack,
    ds_core_stack = decision_support_base_stack,
    ingestion_layer_stack=ingestion_layer_salesforce_stack
)

'''
Layer 6 - TRANSFORM LAYER
'''

# #Salesforce
transform_layer_salesforce_stack = FtTransformLayerSalesforceStack(
    app, 
    id = f"ft-{env}-transform-layer-salesforce-stack",
    env = env,
    region=os.getenv('default_region'),
    email_addresses_to_alert_on_error=os.getenv('email_addresses_to_alert_on_error'),
    bootstrap_stack = decision_support_bootstrap_stack,
    storage_stack = decision_support_persistent_storage_stack,
    ds_core_stack = decision_support_base_stack,
    ingestion_layer_stack=ingestion_layer_salesforce_stack
)

'''
TAGS
'''
Tags.of(app).add("Project", "First Tee Decision Support")
Tags.of(app).add("Deployment", "CDK")
Tags.of(app).add("Repo", "FT-decision-support-cloud")

app.synth()