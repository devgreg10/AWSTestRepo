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

from iac_code.ft_decision_support_base_stack import FtDecisionSupportBaseStack
from iac_code.ft_ingestion_layer_salesforce_stack import FtIngestionLayerSalesforceStack
from iac_code.ft_load_layer_salesforce_stack import FtLoadLayerSalesforceStack

from dotenv import load_dotenv
import os

class FtTransformLayerSalesforceStack(Stack):

    def __init__(self, 
                 scope: Construct, 
                 id: str, 
                 env: str, 
                 region: str,
                 ds_base_stack: FtDecisionSupportBaseStack,
                 ingestion_layer_stack: FtIngestionLayerSalesforceStack, 
                 load_layer_stack: FtLoadLayerSalesforceStack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        elif (env=='uat'):
            load_dotenv(os.path.join(BASEDIR,"../.env.uat"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.dev"))

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

        '''
        TRANFORMATION LAYER
        Loop through salesforce_entities
        '''

        # ZZZ - Move this back the Base Stack 
        self.lambda_execute_db_function = lambda_.Function(self, f"LambdaExecuteDbFunction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            function_name=f"ft-{env}-execute-db-function",
            layers=[ds_base_stack.psycopg2_lambda_layer, load_layer_stack.data_core_lambda_layer],
            #vpc=datawarehouse_vpc,
            #vpc_subnets=ec2.SubnetSelection(
            #    subnets=public_subnets
            #),
            timeout=Duration.seconds(300),
            code=lambda_.Code.from_asset('lambdas/ExecuteDbFunction'),
            handler='lambda_function.lambda_handler'
        )
        # Grant the Lambda function permissions to read the DB Connection Secret
        ds_base_stack.db_secret.grant_read(self.lambda_execute_db_function)

        # Define the parameters as a dictionary
        raw_to_valid_params = {"process_all_raw_records_flag": True}

        for salesforce_entity in ingestion_layer_stack.salesforce_entities:

            entity_name = salesforce_entity["entity_name"]

            # Create Tasks for State Machine
        
            # Step 1: Call ft_ds_admin.raw_to_valid_sf_contact
            execute_raw_to_valid_task = tasks.LambdaInvoke(
                self, f"Execute Raw to Valid Salesforce {entity_name}",
                lambda_function=self.lambda_execute_db_function,
                payload=sfn.TaskInput.from_object({
                    "db_function": f"write_sf_{entity_name}_raw_to_valid",
                    "db_schema": "ft_ds_admin",
                    "input_parameters": raw_to_valid_params,
                    "secret_arn": ds_base_stack.db_secret.secret_arn,
                    "region": region
                }),
                result_path="$.raw_to_valid_result"
            ).add_retry(
                max_attempts=3,
                interval=Duration.seconds(5),
                backoff_rate=2.0
            ).add_catch(
                sfn.Fail(self, f"Stored Procedure raw_to_valid failed Salesforce {entity_name}", error="Stored Procedure raw_to_valid failed", cause="Stored Procedure raw_to_valid failed"),
                errors=["States.ALL"]
            )

            '''
            # Step 2: Call ft_ds_admin.valid_to_refined_sf_contact
            # ft_ds_admin.historical_metric_sf_contact_counts_by_chapter
            execute_valid_to_refined_task = tasks.LambdaInvoke(
                self, f"Execute Valid to Refined Salesforce {entity_name}",
                lambda_function=ds_base_stack.lambda_execute_stored_procedure,
                payload=sfn.TaskInput.from_object({
                    "procedure_name": "ft_ds_admin.valid_to_refined_sf_contact",
                    "secret": sfn.JsonPath.object_at("$.secret.Payload.secret")
                }),
                result_path="$.valid_to_refined_result"
            ).add_retry(
                max_attempts=3,
                interval=Duration.seconds(5),
                backoff_rate=2.0
            ).add_catch(
                sfn.Fail(self, f"Stored Procedure valid_to_refined failed {entity_name}", error="Stored Procedure valid_to_refined failed", cause="Stored Procedure valid_to_refined failed"),
                errors=["States.ALL"]
            )

            # Step 3: Call ft_ds_admin.historical_metric_sf_contact_counts_by_chapter
            execute_historical_metrics_task = tasks.LambdaInvoke(
                self, f"Execute Historical Metrics Salesforce {entity_name}",
                lambda_function=ds_base_stack.lambda_execute_stored_procedure,
                payload=sfn.TaskInput.from_object({
                    "procedure_name": "ft_ds_admin.historical_metric_sf_contact_counts_by_chapter",
                    "secret": sfn.JsonPath.object_at("$.secret.Payload.secret")
                }),
                result_path="$.historical_metrics_result"
            ).add_retry(
                max_attempts=3,
                interval=Duration.seconds(5),
                backoff_rate=2.0
            ).add_catch(
                sfn.Fail(self, f"Stored Procedure historical_metrics failed Salesforce {entity_name}", error="Stored Procedure historical_metrics failed", cause="Stored Procedure historical_metrics failed"),
                errors=["States.ALL"]
            )
            
            '''


            # Define the state machine workflow
            definition = execute_raw_to_valid_task

            # Create the state machine
            state_machine = sfn.StateMachine(
                self, f"FtTransformStateMachineSalesforce{entity_name}",
                state_machine_name=f"ft-{env}-transform-layer-salesforce-{entity_name}",
                definition=definition,
                timeout=Duration.minutes(60)
            )

