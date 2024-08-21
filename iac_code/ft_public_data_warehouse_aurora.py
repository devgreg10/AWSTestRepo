from aws_cdk import (
    aws_rds as rds,
    aws_ec2 as ec2,
    SecretValue,
    Stack,
    RemovalPolicy
)

from constructs import Construct
from dotenv import load_dotenv
import os
import json

class FtPublicDataWarehouseAuroraStack(Stack):

   def __init__(self, scope: Construct, construct_id: str, env: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        if (env == None):
            env = 'no_env'
        
        # Set local variables from environment files to be used later
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
                          ),
                          ec2.Subnet.from_subnet_id(self, "PublicSubnetD", os.getenv('public_subnet_d'))]
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
        Creating PUBLIC access for DB temporarily
        '''
        # Create a security group for the Aurora DB Cluster
        public_db_security_group = ec2.SecurityGroup(
                                        self, 
                                        "ft-" + env + "-public-db-security-group",
                                        vpc=datawarehouse_vpc,
                                        description="Allow public access to Aurora DB Cluster",
                                        allow_all_outbound=True
                                    )

        # Allow incoming traffic from anywhere (0.0.0.0/0) on port 5432 (PostgreSQL default port)
        public_db_security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(5432),
            description="Allow PostgreSQL access from anywhere"
        )

        # create AWS Secret for database username
        database_username = "ft" + env + "publicdbuser"

        '''
        database_credential_secret = secrets.Secret(
            self,
            "ft-" + env + "-public-database-credentials-secret",
            generate_secret_string=secrets.SecretStringGenerator(
                secret_string_template=json.dumps({"username": database_username}),
                exclude_punctuation=True,
                exclude_characters='"@/',
                include_space=False,
                generate_string_key="password"
            )
        )
        '''
        
        # Create Aurora Serverless DB Cluster
        db_cluster = rds.DatabaseCluster(
            self,
            "ft-" + env + "-public-aurora-db-cluster",
            vpc=datawarehouse_vpc,
            vpc_subnets=ec2.SubnetSelection(
                # subnets=private_subnets
                subnets=public_subnets
            ),
            # credentials=rds.Credentials.from_secret(database_credential_secret),
            credentials=rds.Credentials.from_username(
                username=database_username,
                password=SecretValue.unsafe_plain_text("tempdbpassword_kirkcousinsqbatlantafalc0ns")
            ),
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_15_3 
            ),
            default_database_name="decision_support",
            writer=rds.ClusterInstance.serverless_v2(
                "ft-" + env + "-public-writer-instance",
                enable_performance_insights=True,
                publicly_accessible=True
            ),
            serverless_v2_min_capacity=0.5,
            serverless_v2_max_capacity=8,
            security_groups=[public_db_security_group]
        )
        
        
        

        # Use DESTROY in Dev environment only only
        if (env=="dev"):
            db_cluster.removal_policy=RemovalPolicy.DESTROY 
            
