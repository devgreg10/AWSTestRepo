from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Stack,
    Duration
)

from constructs import Construct

from dotenv import load_dotenv
import os

class FtTransformLayerStack(Stack):

    def __init__(self, scope: Construct, id: str, env: str, secret_arn: str, secret_region: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.non_prod"))

        vpc_name = os.getenv('vpc_name')
        vpc_id = os.getenv('vpc_id')
        public_subnets = [ec2.Subnet.from_subnet_attributes(
                            self, 
                            "PublicSubnetA", 
                            subnet_id=os.getenv('public_subnet_a'),
                            availability_zone=os.getenv("public_subnet_a_az")
                          )]
        private_subnets = [ec2.Subnet.from_subnet_id(self, "PrivateSubnetA", os.getenv('private_subnet_a')),
                           ec2.Subnet.from_subnet_id(self, "PrivateSubnetD", os.getenv('private_subnet_d'))]

        # Get a reference to the existing VPC
        datawarehouse_vpc = ec2.Vpc.from_vpc_attributes(
            self,
            "ExistingVPC",
            vpc_id=vpc_id,
            availability_zones=["us-east-1a"]
        )

        # Define a Lambda Layer for the psycopg2 library
        psycopg2_layer = lambda_.LayerVersion(
            self, 'Psycopg2Layer',
            code=lambda_.Code.from_asset('lambda_layers/API_Layer'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8, lambda_.Runtime.PYTHON_3_9],
            description="A layer for psycopg2",
        )

        lambda_retrieve_secrets = lambda_.Function(self, "LambdaRetrieveSecrets",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name="ft-" + env + "-retrieve-secrets",
            timeout=Duration.seconds(30),
            code=lambda_.Code.from_asset('lambdas/RetrieveSecrets'),
            handler='lambda_function.lambda_handler',
            environment={
                "DB_SECRET_ARN": secret_arn,
                "DB_SECRET_REGION": secret_region,
            }
        )

        # Grant the Lambda function permissions to read from Secrets 
        lambda_retrieve_secrets.add_to_role_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[secret_arn],
            )
        )

        lambda_execute_stored_procedure = lambda_.Function(self, "LambdaExecuteStoredProcedure",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name="ft-" + env + "-execute-stored-procedure",
            layers=[psycopg2_layer],
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(300),
            code=lambda_.Code.from_asset('lambdas/ExecuteStoredProcedure'),
            handler='lambda_function.lambda_handler'
        )

        # Create Tasks for State Machine

        # Step 1: Retrieve Secrets
        retrieve_secrets_task = tasks.LambdaInvoke(
            self, "Retrieve Secrets",
            lambda_function=lambda_retrieve_secrets,
            result_path="$.secret"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "RetrieveSecretsFailed", error="RetrieveSecretsFailed", cause="Failed to retrieve secrets"),
            errors=["States.ALL"]
        )

        # Step 2: Call ft_ds_admin.raw_to_valid_sf_contact
        execute_raw_to_valid_task = tasks.LambdaInvoke(
            self, "Execute Raw to Valid",
            lambda_function=lambda_execute_stored_procedure,
            payload=sfn.TaskInput.from_object({
                "procedure_name": "ft_ds_admin.raw_to_valid_sf_contact",
                "secret": "$.secret.Payload.secret"
            }),
            result_path="$.raw_to_valid_result"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "Stored Procedure ft_ds_admin.raw_to_valid_sf_contact failed", error="Stored Procedure ft_ds_admin.raw_to_valid_sf_contact failed", cause="Stored Procedure ft_ds_admin.raw_to_valid_sf_contact failed"),
            errors=["States.ALL"]
        )

        # Step 3: Call ft_ds_admin.valid_to_refined_sf_contact
        # ft_ds_admin.historical_metric_sf_contact_counts_by_chapter
        execute_valid_to_refined_task = tasks.LambdaInvoke(
            self, "Execute Valid to Refined",
            lambda_function=lambda_execute_stored_procedure,
            payload=sfn.TaskInput.from_object({
                "procedure_name": "ft_ds_admin.valid_to_refined_sf_contact",
                "secret": "$.secret.Payload.secret"
            }),
            result_path="$.valid_to_refined_result"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "Stored Procedure ft_ds_admin.valid_to_refined_sf_contact failed", error="Stored Procedure ft_ds_admin.raw_to_valid_sf_contact failed", cause="Stored Procedure ft_ds_admin.raw_to_valid_sf_contact failed"),
            errors=["States.ALL"]
        )

        # Step 4: Call ft_ds_admin.historical_metric_sf_contact_counts_by_chapter
        execute_historical_metrics_task = tasks.LambdaInvoke(
            self, "Execute Historical Metrics",
            lambda_function=lambda_execute_stored_procedure,
            payload=sfn.TaskInput.from_object({
                "procedure_name": "ft_ds_admin.historical_metric_sf_contact_counts_by_chapter",
                "secret": "$.secret.Payload.secret"
            }),
            result_path="$.historical_metrics_result"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self, "Stored Procedure ft_ds_admin.historical_metric_sf_contact_counts_by_chapter failed", error="Stored Procedure ft_ds_admin.historical_metric_sf_contact_counts_by_chapter failed", cause="Stored Procedure ft_ds_admin.historical_metric_sf_contact_counts_by_chapter failed"),
            errors=["States.ALL"]
        )


         # Define the state machine workflow
        definition = retrieve_secrets_task.next(execute_raw_to_valid_task).next(execute_valid_to_refined_task).next(execute_historical_metrics_task)

        # Create the state machine
        state_machine = sfn.StateMachine(
            self, "FtTransformStateMachine",
            state_machine_name="ft-" + env + "-transform-layer",
            definition=definition,
            timeout=Duration.minutes(15)
        )

        