from aws_cdk import (
    aws_lambda as lambda_,
    aws_secretsmanager as secretsmanager,
    aws_s3 as s3,
    aws_iam as iam,
    Stack,
    RemovalPolicy
)

from constructs import Construct

from cloud.layer_1_bootstrap.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack
from cloud.layer_2_storage.ft_decision_support_persistent_storage_stack import FtDecisionSupportPersistentStorageStack

class FtDecisionSupportCoreStack(Stack):

    def __init__(self, 
                 scope: Construct, 
                 id: str, 
                 env: str, 
                 version_number: str, 
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
        
        

