from aws_cdk import (
    aws_lambda as lambda_,
    aws_secretsmanager as secretsmanager,
    aws_s3 as s3,
    aws_iam as iam,
    Stack,
    SecretValue,
    Duration
)

from constructs import Construct

class FtDecisionSupportBaseStack(Stack):

    def __init__(self, scope: Construct, id: str, env: str, secret_region: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        '''
        DB Connection String - AWS Secret
        '''
        # Store the known Aurora PostgreSQL credentials in AWS Secrets Manager
        self.db_secret = secretsmanager.Secret(self, "AuroraDBSecret",
            description="Aurora PostgreSQL DB credentials",
            secret_name="ft-aurora-postgres-connection-secret",
            secret_string_value=SecretValue.plain_text(
                '{"username":"ftdevpublicdbuser","password":"tempdbpassword_kirkcousinsqbatlantafalc0ns","dbname":"postgres","host":"firsttee-cdk-public-data-ftdevpublicauroradbclust-j3otj0e0ak2o.cluster-c5qwcaqekjc2.us-east-1.rds.amazonaws.com"}'
            )
        )

        '''
        LAMBDAs
        '''
        # Define a Lambda Layer for the psycopg2 library
        self.psycopg2_lambda_layer = lambda_.LayerVersion(
            self, f'ft-{env}-lambda-layer-psycopg2',
            code=lambda_.Code.from_asset('lambda_layers/API_Layer'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8, lambda_.Runtime.PYTHON_3_9],
            description="A layer for psycopg2",
        )

        # Define Lambda to retrieve secrets
        self.lambda_retrieve_secrets = lambda_.Function(self, "LambdaRetrieveSecrets",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name=f"ft-{env}-retrieve-secrets",
            #layers=[psycopg2_layer],
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(30),
            code=lambda_.Code.from_asset('lambdas/RetrieveSecrets'),
            handler='lambda_function.lambda_handler',
            environment={
                "DB_SECRET_ARN": self.db_secret.secret_arn,
                "DB_SECRET_REGION": secret_region,
            }
        )

        # Grant the Lambda function permission to retrieve the DB secret
        self.db_secret.grant_read(self.lambda_retrieve_secrets)

        # Define a Lambda that can execute a stored procedure
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
        DATA LAKE - S3 Bucket
        '''
        self.data_lake_bucket = s3.Bucket(self, 
            "FTDevDataLakeBucket",
            bucket_name=f"ft-{env}-data-lake"
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

