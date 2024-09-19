from aws_cdk import (
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_secretsmanager as secrets,
    aws_iam as iam,
    aws_s3 as s3,
    aws_lambda as _lambda,
    custom_resources as cr,
    Stack,
    RemovalPolicy
)

from constructs import Construct
from dotenv import load_dotenv
import os
import json

class FtDecisionSupportBootstrapStack(Stack):

   def __init__(self, 
                scope: Construct, 
                id: str, 
                env: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        if (env == None):
            env = 'no_env'
        
        # Set local variables from environment files to be used later
        
        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        elif (env=='uat'):
            load_dotenv(os.path.join(BASEDIR,"../.env.uat"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.dev"))

        '''
        The purpose of this Bootstrap Stack is to identify AWS Resources not created by Decision Support developers
        '''
        vpc_id = os.getenv('vpc_id')
        public_subnet_ids = json.loads(os.getenv('public_subnet_ids'))
        private_subnet_ids = json.loads(os.getenv('private_subnet_ids'))
        availability_zones = json.loads(os.getenv('availability_zones'))
        vpc_cidr_block = os.getenv('vpc_cidr_block')
        
        self.decision_support_vpc = ec2.Vpc.from_vpc_attributes(
            self,
            "DecisionSupportVPC",
            vpc_id = vpc_id,
            public_subnet_ids = public_subnet_ids,
            private_subnet_ids = private_subnet_ids,
            availability_zones = availability_zones,
            vpc_cidr_block = vpc_cidr_block
        )
        

