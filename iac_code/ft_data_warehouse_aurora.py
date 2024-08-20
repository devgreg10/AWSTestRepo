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

class FtDataWarehouseAuroraStack(Stack):

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

        # create AWS Secret for database username
        database_username = "ft" + env + "dbuser"

        database_credential_secret = secrets.Secret(
            self,
            "ft-" + env + "-database-credentials-secret",
            generate_secret_string=secrets.SecretStringGenerator(
                secret_string_template=json.dumps({"username": database_username}),
                exclude_punctuation=True,
                exclude_characters='"@/',
                include_space=False,
                generate_string_key="password"
            )
        )

        '''
        ssm.StringParameter(
            self,
            "DBCredentialsArn",
            parameter_name="ft-" + env + "-rds-credentials-arn",
            string_value=database_credential_secret.secret_arn
        )
        '''

        
        # Create Aurora Serverless DB Cluster
        db_cluster = rds.DatabaseCluster(
            self,
            "ft-" + env + "-aurora-db-cluster",
            vpc=datawarehouse_vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=private_subnets
            ),
            credentials=rds.Credentials.from_secret(database_credential_secret),
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_15_3 
            ),
            default_database_name="decision_support",
            readers=[
                rds.ClusterInstance.serverless_v2(
                    "ft-" + env + "-reader-instance-1", 
                    scale_with_writer=True,
                    enable_performance_insights=True
                ),
                rds.ClusterInstance.serverless_v2(
                    "ft-" + env + "-reader-instance-2",
                    enable_performance_insights=True
                )
            ],
            writer=rds.ClusterInstance.serverless_v2(
                "ft-" + env + "-writer-instance",
                enable_performance_insights=True
            ),
            serverless_v2_min_capacity=0.5,
            serverless_v2_max_capacity=8
            #security_groups=[rds_db_connection_security_group]
        )
        
        
        """
        # ServerlessCluster creates an Aurora Serverless v1 Cluster!!!
        serverless_cluster = rds.ServerlessCluster(
            self,
            "ft-" + env + "-aurora-serverless-cluster",
            vpc=datawarehouse_vpc,
            credentials=rds.Credentials.from_secret(database_credential_secret),
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_15_3 
            ),
            scaling=rds.ServerlessScalingOptions(
                auto_pause=Duration.minutes(10),  # Auto-pause after 10 minutes
                min_capacity=rds.AuroraCapacityUnit.ACU_2,
                max_capacity=rds.AuroraCapacityUnit.ACU_8
            ),
            enable_data_api=True,  # Enable Data API for Aurora Serverless
            default_database_name="decision_support",
        )
        """

         # Define a security group for the RDS Proxy
        rds_proxy_sg = ec2.SecurityGroup(
            self, 
            "ft-" + env + "-rds-proxy-security-group",
            vpc=datawarehouse_vpc,
            description="Security group for RDS Proxy",
            allow_all_outbound=True
        )

        # Allow inbound traffic on the database port from your IP address
        rds_proxy_sg.add_ingress_rule(
            # peer=ec2.Peer.ipv4(datawarehouse_vpc.vpc_cidr_block), # Allows access from within the VPC
            # RuntimeError: Error: Cannot perform this operation: 'vpcCidrBlock' was not supplied when creating this VPC
            peer=ec2.Peer.any_ipv4(),  # ZZZ - Wide open, need to figure out better security here
            connection=ec2.Port.tcp(5432)  # PostgreSQL port
        )
        
        # Create an RDS Proxy for the Aurora DB
        rds_proxy = db_cluster.add_proxy(
            "ft-" + env + "-rds-proxy",
            secrets=[database_credential_secret],
            vpc=datawarehouse_vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=private_subnets
            ),
            security_groups=[rds_proxy_sg],
            iam_auth=True  # Enable IAM authentication
        )

        '''
        rds_proxy = rds.DatabaseProxy(
            self, 
            "ft-" + env + "-rds-database-proxy",
            proxy_target=rds.ProxyTarget.from_cluster(serverless_cluster),
            secrets=[database_credential_secret],
            vpc=datawarehouse_vpc,
            iam_auth=True,
            vpc_subnets=ec2.SubnetSelection(
                subnets=datawarehouse_vpc.private_subnets
            ),
            security_groups=[rds_proxy_sg]
        )
        '''

        # Create a security group for the bastion host
        bastion_sg = ec2.SecurityGroup(
            self, 
            "ft-" + env + "-bastion-security-group",
            vpc=datawarehouse_vpc,
            description="Security group for Bastion Host",
            allow_all_outbound=True)
        
        # Allow the bastion host to connect to the RDS Proxy on port 5432 explicitly
        rds_proxy_sg.add_ingress_rule(
            peer=bastion_sg,
            connection=ec2.Port.tcp(5432),  # Explicitly allowing port 5432
            description="Allow Bastion Host to connect to RDS Proxy on port 5432"
        )
        
        # Allow inbound SSH access to Bastion
        bastion_sg.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),  # ZZZ - Wide open, need to figure out better security here
            connection=ec2.Port.tcp(22)  # SSH port
        )
        
        # Create a new EC2 key name pair
        new_key_pair_name = "ft-" + env + "-key-pair"
        
        new_key_pair = ec2.CfnKeyPair(
            self,
            "NewKeyNamePair",
            key_name=new_key_pair_name
        )

        # Launch an EC2 instance as the bastion host
        bastion_host = ec2.Instance(
            self, 
            "ft-" + env + "-bastion-host",
            instance_type=ec2.InstanceType("t2.micro"),
            machine_image=ec2.MachineImage.latest_amazon_linux(),
            vpc=datawarehouse_vpc,
            key_name=new_key_pair_name,
            security_group=bastion_sg,
            vpc_subnets=ec2.SubnetSelection(
                subnets=public_subnets
            )
        )

        # Use DESTROY in Dev environment only only
        if (env=="dev"):
            db_cluster.removal_policy=RemovalPolicy.DESTROY 
            rds_proxy.apply_removal_policy(RemovalPolicy.DESTROY)

        # Output the RDS Proxy endpoint
        # CfnOutput(self, "RDSProxyEndpoint", value=db_cluster..endpoint)

        # Output the Aurora Cluster endpoint
        # CfnOutput(self, "AuroraClusterEndpoint", value=db_cluster.cluster_endpoint.hostname)
