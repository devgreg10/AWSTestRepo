from aws_cdk import (
    aws_cognito as cognito,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_secretsmanager as secrets,
    aws_ssm as ssm,
    Duration,
    Environment,
    Stack,
    CfnOutput,
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
        
        '''
        # define subnets
        public_subnet=ec2.SubnetConfiguration(
            subnet_type=ec2.SubnetType.PUBLIC,
            name="ft-" + env + "-public-subnet"            
            # cidr_mask=24
        )

        private_subnet=ec2.SubnetConfiguration(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            name="ft-" + env + "-private-subnet-with-nat"
            #cidr_mask=24
        )

        # create VPC
        datawarehouse_vpc = ec2.Vpc(
            self, 
            "ft-" + env + "-data-warehouse-vpc",
            ip_addresses=ec2.IpAddresses.cidr('10.0.0.0/16'),
            #availability_zones=['us-east-1a','us-east-1b'],
            availability_zones=['us-east-1a','us-east-1b'],
            nat_gateways=1,
            subnet_configuration=[public_subnet, private_subnet]
        )
        '''

        '''
        # dev/uat VPC
        non_prod_vpc_name='pgatour-firsttee-dev-datalake-VPC'
        non_prod_vpc_id='vpc-079fd4f484100b2eb'
        non_prod_public_subnets = [ec2.Subnet.from_subnet_id(self, "PublicSubnetA", "subnet-012d037b1ad02d93e")]
        non_prod_private_subnets = [ec2.Subnet.from_subnet_id(self, "PrivateSubnetA", "subnet-0300505f922f7bae5"),
                                    ec2.Subnet.from_subnet_id(self, "PrivateSubnetD", "subnet-077e463bade38c02a")]

        # prod VPC
        prod_vpc_name='pgatour-firsttee-prod-datalake-VPC'
        prod_vpc_id='vpc-0ee714c5f331c77d2'
        prod_public_subnets = [ec2.Subnet.from_subnet_id(self, "PublicSubnetA", "subnet-098513801425ff4d2")]
        prod_private_subnets = [ec2.Subnet.from_subnet_id(self, "PrivateSubnetA", "subnet-044363d0790483b33"),
                                    ec2.Subnet.from_subnet_id(self, "PrivateSubnetD", "subnet-0b322844d620f011b")]
        '''
        BASEDIR = os.path.abspath(os.path.dirname(__file__))
        if (env=='prod'):
            load_dotenv(os.path.join(BASEDIR, "../.env.prod"))
        else:
            load_dotenv(os.path.join(BASEDIR,"../.env.non_prod"))

        vpc_name = os.getenv('vpc_name')
        vpc_id = os.getenv('vpc_id')
        public_subnets = [ec2.Subnet.from_subnet_id(self, "PublicSubnetA", os.getenv('public_subnet_a'))]
        private_subnets = [ec2.Subnet.from_subnet_id(self, "PrivateSubnetA", os.getenv('private_subnet_a')),
                           ec2.Subnet.from_subnet_id(self, "PrivateSubnetD", os.getenv('private_subnet_d'))]

        '''
        datawarehouse_vpc = ec2.Vpc.from_lookup(
            self,
            "ExistingVPC",
            vpc_id=vpc_id,
            vpc_name=vpc_name
        )
        '''

        datawarehouse_vpc = ec2.Vpc.from_vpc_attributes(
            self,
            "ExistingVPC",
            vpc_id=vpc_id,
            availability_zones=["us-east-1a"]
        )

        
        
    
        '''

        # add interface endpoint for secret manager
        datawarehouse_vpc.add_interface_endpoint(
            "ft-" + env + "-secret-manager-endpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER
        )

        # create security group to allow other services to connect to RDS Proxy 
        rds_proxy_connection_security_group = ec2.SecurityGroup(
            self,
            "ft-" + env + "-rds-proxy-connection-security-group",
            vpc=datawarehouse_vpc
        )

        # create security group to allow Proxy to connect to DB
        rds_db_connection_security_group = ec2.SecurityGroup(
            self,
            "ft-" + env + "-db-connection-security-group",
            vpc=datawarehouse_vpc
        )

        rds_db_connection_security_group.add_ingress_rule(
            rds_db_connection_security_group,
            ec2.Port.tcp(5432),
            "allow db connection"
        )

        rds_db_connection_security_group.add_ingress_rule(
            rds_proxy_connection_security_group,
            ec2.Port.tcp(5432),
            "allow proxy connection"
        )
        '''

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
            peer=ec2.Peer.any_ipv4(), # ZZZ - allows all inbound traffic and will need to change
            connection=ec2.Port.tcp(5432)  # PostgreSQL port
        )
        
        # Create an RDS Proxy for the Aurora DB
        
        db_cluster.add_proxy(
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
        
        # Use DESTROY in Dev environment only only
        if (env=="dev"):
            db_cluster.removal_policy=RemovalPolicy.DESTROY 
            #serverless_cluster.removal_policy=RemovalPolicy.DESTROY 
            #datawarehouse_vpc.apply_removal_policy(RemovalPolicy.DESTROY)
            


        # Output the RDS Proxy endpoint
        # CfnOutput(self, "RDSProxyEndpoint", value=db_cluster..endpoint)

        # Output the Aurora Cluster endpoint
        # CfnOutput(self, "AuroraClusterEndpoint", value=db_cluster.cluster_endpoint.hostname)
