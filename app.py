import os

import aws_cdk as cdk

'''
from iac_code.ft_create_secret import FtCreateSecretsStack
from iac_code.ft_load_layer_salesforce_contact_stack import FtLoadLayerSalesforceContactStack
from iac_code.ft_transform_layer_salesforce_contact_stack import FtTransformLayerSalesforceContactStack
from iac_code.ft_ingestion_layer_salesforce_contact_stack import FtSalesforceContactIngestionLayerStack
'''
from iac_code.ft_decision_support_base_stack import FtDecisionSupportBaseStack
from iac_code.ft_salesforce_entity_stack import FtSalesforceEntityStack

from iac_code.appflow.tasks.ft_salesforce_contact_tasks import FtSalesforceContactAppFlowTasks
from iac_code.appflow.tasks.ft_salesforce_listing_session_tasks import FtSalesforceListingSessionAppFlowTasks
from iac_code.appflow.tasks.ft_salesforce_listing_tasks import FtSalesforceListingAppFlowTasks
from iac_code.appflow.tasks.ft_salesforce_session_registration_tasks import FtSalesforceSessionRegistrationAppFlowTasks

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


decision_support_base_stack = FtDecisionSupportBaseStack(
    app, 
    id=f"ft-{env}-decision-support-base-stack", 
    env=env, 
    secret_region=os.getenv('db_connection_secret_region')
)

'''
SALESFORCE
'''
# CONTACT
entity_name = "contact"
salesforce_object = "Contact"
appflow_tasks = FtSalesforceContactAppFlowTasks(app, "SaleforceContactTasks")

salesforce_contact_stack = FtSalesforceEntityStack(
    app, 
    id = f"ft-{env}-salesforce-{entity_name}-stack",
    env = env,
    entity_name = entity_name,
    salesforce_object = salesforce_object,
    app_flow_tasks = appflow_tasks.get_tasks(),
    commit_batch_size = os.getenv('load_layer_commit_batch_size'),
    concurrent_lambdas = os.getenv('load_layer_salesforce_concurrent_lambdas'),
    ds_base_stack = decision_support_base_stack
)

# LISTING
entity_name = "listing"
salesforce_object = "Listing__c"
appflow_tasks = FtSalesforceListingAppFlowTasks(app, "SaleforceListingTasks")

salesforce_listing_stack = FtSalesforceEntityStack(
    app, 
    id = f"ft-{env}-salesforce-{entity_name}-stack",
    env = env,
    entity_name = entity_name,
    salesforce_object = salesforce_object,
    app_flow_tasks = appflow_tasks.get_tasks(),
    commit_batch_size = os.getenv('load_layer_commit_batch_size'),
    concurrent_lambdas = os.getenv('load_layer_salesforce_concurrent_lambdas'),
    ds_base_stack = decision_support_base_stack
)


# LISTING SESSION
entity_name = "listing-session"
salesforce_object = "Listing_Session__c"
appflow_tasks = FtSalesforceListingSessionAppFlowTasks(app, "SaleforceListingSessionTasks")

salesforce_listing_stack = FtSalesforceEntityStack(
    app, 
    id = f"ft-{env}-salesforce-{entity_name}-stack",
    env = env,
    entity_name = entity_name,
    salesforce_object = salesforce_object,
    app_flow_tasks = appflow_tasks.get_tasks(),
    commit_batch_size = os.getenv('load_layer_commit_batch_size'),
    concurrent_lambdas = os.getenv('load_layer_salesforce_concurrent_lambdas'),
    ds_base_stack = decision_support_base_stack
)

# SESSION REGISTRATION
entity_name = "session-registration"
salesforce_object = "Session_Registration__c"
appflow_tasks = FtSalesforceSessionRegistrationAppFlowTasks(app, "SaleforceSessionRegistrationTasks")

salesforce_listing_stack = FtSalesforceEntityStack(
    app, 
    id = f"ft-{env}-salesforce-{entity_name}-stack",
    env = env,
    entity_name = entity_name,
    salesforce_object = salesforce_object,
    app_flow_tasks = appflow_tasks.get_tasks(),
    commit_batch_size = os.getenv('load_layer_commit_batch_size'),
    concurrent_lambdas = os.getenv('load_layer_salesforce_concurrent_lambdas'),
    ds_base_stack = decision_support_base_stack
)


'''
create_secret_stack = FtCreateSecretsStack(app, f"ft-{env}-create-db-secret")

ingestion_layer_salesforce_contact_stack = FtSalesforceContactIngestionLayerStack(app, f"ft-{env}-ingestion-layer-salesforce-contact-stack", 
    env=env,
    datalake_bucket_folder=os.getenv('load_layer_salesforce_contact_s3_bucket_folder'))

load_layer_salesforce_contact_stack = FtLoadLayerSalesforceContactStack(app, f"ft-{env}-load-layer-salesforce-contact-stack",
    env=env,
    bucket_folder=os.getenv('load_layer_salesforce_contact_s3_bucket_folder'),
    concurrent_lambdas=os.getenv('load_layer_salesforce_concurrent_lambdas'),
    commit_batch_size=os.getenv('load_layer_commit_batch_size'),
    secret_region=os.getenv('db_connection_secret_region'),
    secret_layer_stack=create_secret_stack,
    ingestion_layer_stack=ingestion_layer_salesforce_contact_stack
)

load_layer_salesforce_contact_stack.add_dependency(create_secret_stack)
load_layer_salesforce_contact_stack.add_dependency(ingestion_layer_salesforce_contact_stack)

tranform_layer_salesforce_contact_stack = FtTransformLayerSalesforceContactStack(app, f"ft-{env}-tranform-layer-salesforce-contact-stack",
    env=env,
    load_layer_stack=load_layer_salesforce_contact_stack
)

tranform_layer_salesforce_contact_stack.add_dependency(load_layer_salesforce_contact_stack)

'''


cdk.Tags.of(app).add("Project", "First Tee Decision Support")
cdk.Tags.of(app).add("Deployment", "CDK")
cdk.Tags.of(app).add("Repo", "FT-decision-support-cloud")

app.synth()