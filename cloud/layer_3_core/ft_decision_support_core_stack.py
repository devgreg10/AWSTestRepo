from aws_cdk import (
    aws_lambda as lambda_,
    aws_secretsmanager as secrets,
    aws_s3 as s3,
    aws_iam as iam,
    custom_resources as cr,
    Stack,
    CustomResource,
    Duration
)
from aws_cdk.aws_iam import PolicyStatement
import json
from constructs import Construct

from cloud.layer_1_bootstrap.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack
from cloud.layer_2_storage.ft_decision_support_persistent_storage_stack import FtDecisionSupportPersistentStorageStack

class FtDecisionSupportCoreStack(Stack):

    def __init__(self, 
                 scope: Construct, 
                 id: str, 
                 env: str, 
                 version_number: str, 
                 storage_stack: FtDecisionSupportPersistentStorageStack,
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        '''
        LAMBDA LAYERS
        '''
        # Define a Lambda Layer for the base libraries
        self.psycopg2_lambda_layer = lambda_.LayerVersion(
            self, f'ft-{env}-lambda-layer-decision-support-base',
            code=lambda_.Code.from_asset('cloud/lambda_layers/API_Layer'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8, lambda_.Runtime.PYTHON_3_9],
            description="A layer for psycopg2, pytz, attrs, dataclasses_json",
            layer_version_name=f'ft-{env}-aws-decision-support-lambda-layer-{version_number.replace(".", "-")}'
        )

        '''
        LAMBDAS
        '''
        #__________________________________________________________________________________________
        # Create 2 additional users for the database and save passwords in Secrets Manager
        #__________________________________________________________________________________________

        bi_dashboard_username= f"ft_{env}_data_warehouse_bi_dashboard_user"

        # Secret for BI Dashboard user
        bi_dashboard_user_secret = secrets.Secret(
            self,
            "BIDashboardUserSecret",
            secret_name=f"ft-{env}-data-warehouse-db-bi-dashboard-user-secret",
            generate_secret_string=secrets.SecretStringGenerator(
                secret_string_template=json.dumps({
                    "username": bi_dashboard_username
                }),
                generate_string_key="password",
                exclude_punctuation=True,
                exclude_characters='"@/',
                include_space=False
            )
        )

        dev_username = f"ft_{env}_data_warehouse_dev_user"

        # Secret for Dev user
        dev_user_secret = secrets.Secret(
            self,
            "DevUserSecret",
            secret_name=f"ft-{env}-data-warehouse-db-dev-user-secret",
            generate_secret_string=secrets.SecretStringGenerator(
                secret_string_template=json.dumps({
                    "username": dev_username
                }),
                generate_string_key="password",
                exclude_punctuation=True,
                exclude_characters='"@/',
                include_space=False
            )
        )

        # Lambda function to create the users
        create_db_users_lambda = lambda_.Function(
            self, 
            "CreateDbUsersLambda",
            function_name=f"ft-{env}-create-db-users-lambda",
            runtime=lambda_.Runtime.PYTHON_3_8,
            layers=[self.psycopg2_lambda_layer],
            code=lambda_.Code.from_asset('cloud/lambdas/DbUsers'),
            handler='lambda_function.lambda_handler',
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "LOG_LEVEL": "INFO"
            }
        )

        # Allow it to read secrets
        create_db_users_lambda.add_to_role_policy(PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[
                storage_stack.db_master_user_secret.secret_arn,
                bi_dashboard_user_secret.secret_arn,
                dev_user_secret.secret_arn,
            ]
        ))

        # Custom Resource Provider
        provider = cr.Provider(
            self, "CreateDbUsersProvider",
            provider_function_name=f"ft-{env}-create-db-users-provider",
            on_event_handler=create_db_users_lambda
        )

        # Custom Resource that triggers Lambda
        CustomResource(
            self, "CreateDbUsersCustomResource",
            service_token=provider.service_token,
            properties={
                "DBHost": storage_stack.data_warehouse_db_cluster.cluster_endpoint.hostname,
                "DBPort": 5432,
                "DBName": storage_stack.default_database_name,
                "MasterSecretArn": storage_stack.db_master_user_secret.secret_arn,
                "BIUserSecretArn": bi_dashboard_user_secret.secret_arn,
                "DevUserSecretArn": dev_user_secret.secret_arn
            }
        )
        

