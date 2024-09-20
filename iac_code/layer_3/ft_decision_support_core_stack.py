from aws_cdk import (
    aws_lambda as lambda_,
    aws_secretsmanager as secretsmanager,
    aws_s3 as s3,
    aws_iam as iam,
    Stack,
    RemovalPolicy
)

from constructs import Construct

from iac_code.layer_1.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack
from iac_code.layer_2.ft_decision_support_persistent_storage_stack import FtDecisionSupportPersistentStorageStack

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
            code=lambda_.Code.from_asset('lambda_layers/API_Layer'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8, lambda_.Runtime.PYTHON_3_9],
            description="A layer for psycopg2, pytz, attrs, dataclasses_json",
            layer_version_name=f'ft-{env}-aws-decision-support-lambda-layer-{version_number.replace(".", "-")}'
        )

        '''
        LAMBDAS
        '''
       
        # Define a Lambda that can execute a stored procedure
        # ZZZ - Replace this commented out lambda with the LambdaExecuteFunction from Tranform Layer Stack
        '''
        self.lambda_execute_stored_procedure = lambda_.Function(self, "LambdaExecuteStoredProcedure",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name=f"ft-{env}-execute-stored-procedure",
            layers=[self.psycopg2_lambda_layer],
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(300),
            code=lambda_.Code.from_asset('lambdas/ExecuteStoredProcedure'),
            handler='lambda_function.lambda_handler'
        )
        '''
        
        '''
        DATA LAKE - S3 Bucket
        '''
        data_lake_bucket_name = f"ft-{env}-data-lake"
        
        self.data_lake_bucket = s3.Bucket(self, 
            "FTDevDataLakeBucket",
            bucket_name=data_lake_bucket_name,
            removal_policy=RemovalPolicy.DESTROY
        )

        # DataLake - Define S3 bucket policy for AppFlow
        appflow_policy_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            principals=[iam.ServicePrincipal("appflow.amazonaws.com")],
            actions=[
                "s3:PutObject",
                "s3:GetBucketAcl",
                "s3:PutObjectAcl"
            ],
            resources=[
                self.data_lake_bucket.bucket_arn,  # Bucket itself
                f"{self.data_lake_bucket.bucket_arn}/*"  # All objects in the bucket
            ]
        )

        # Attach AppFlow policy to the bucket
        self.data_lake_bucket.add_to_resource_policy(appflow_policy_statement)

        # Define bucket policy for AWS Management Console users to move files
        move_files_policy_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            principals=[iam.AccountRootPrincipal()],  # Allows any user from the account
            actions=[
                "s3:ListBucket",    # List objects in the bucket
                "s3:GetObject",     # Get objects (needed for move)
                "s3:PutObject",     # Put objects (needed for move)
                "s3:DeleteObject"   # Delete objects (needed for move)
            ],
            resources=[
                self.data_lake_bucket.bucket_arn,        # Required for s3:ListBucket
                f"{self.data_lake_bucket.bucket_arn}/*"  # All objects in the bucket
            ]
        )

        # Attach the move files policy to the bucket
        self.data_lake_bucket.add_to_resource_policy(move_files_policy_statement)

