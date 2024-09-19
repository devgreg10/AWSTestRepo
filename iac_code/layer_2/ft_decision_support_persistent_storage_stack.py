from aws_cdk import (
    aws_iam as iam,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_secretsmanager as secrets,
    aws_s3 as s3,
    Stack,
    RemovalPolicy
)

from constructs import Construct
from dotenv import load_dotenv

import os
import json

from iac_code.layer_1.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack

class FtDecisionSupportPersistentStorageStack(Stack):

   def __init__(self, 
                scope: Construct, 
                id: str, 
                env: str, 
                boostrap_stack: FtDecisionSupportBootstrapStack, **kwargs):
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
        EC2 Key Pair creation
        '''
        # # Boto3 EC2 client for key pair creation
        # ec2_client = boto3.client('ec2')

        # # Method to create an EC2 Key Pair programmatically
        # def create_key_pair(key_name: str):
        #     response = ec2_client.create_key_pair(KeyName=key_name)
        #     private_key = response['KeyMaterial']
        #     return private_key

        # # Create the key pair programmatically
        # key_name = f"ft-{env}-bastion-key"
        # private_key = create_key_pair(key_name)

        # # Upload the private key to an S3 bucket
        # bucket = s3.Bucket(
        #     self, 
        #     "BastionKeyBucket", 
        #     bucket_name=f"ft-{env}-bastion-key-bucket")
        # s3_key_name = f"{key_name}.pem"
        # bucket.put_object(Key=s3_key_name, Body=private_key)

        '''
        Security Groups
        '''
        # Security Group for Aurora PostgreSQL DB
        rds_sg = ec2.SecurityGroup(
            self, 
            "RdsSecurityGroup",
            security_group_name=f"ft-{env}-rds-security-group",
            vpc=boostrap_stack.decision_support_vpc,
            description="Security group for Aurora PostgreSQL",
            allow_all_outbound=True
        )

        # Allow inbound PostgreSQL traffic on port 5432 within the VPC
        rds_sg.add_ingress_rule(
            peer = ec2.Peer.ipv4(boostrap_stack.decision_support_vpc.vpc_cidr_block), 
            connection = ec2.Port.tcp(5432), 
            description ="Allow PostgreSQL traffic")

        # Security Group for Bastion Host
        bastion_sg = ec2.SecurityGroup(
            self, "BastionSecurityGroup",
            vpc=boostrap_stack.decision_support_vpc,
            description="Security group for Bastion host",
            allow_all_outbound=True
        )
        
        # Allow SSH access from anywhere (0.0.0.0/0) 
        bastion_sg.add_ingress_rule(
            peer = ec2.Peer.any_ipv4(), 
            connection = ec2.Port.tcp(22), 
            description = "Allow SSH access to bastion")

        # Create a new Secret for the RDS Master user
        db_secret = secrets.Secret(
            self, 
            "DBSecret",
            secret_name = f"ft-{env}-datawarehouse-db-master-user-secret",
            generate_secret_string=secrets.SecretStringGenerator(
                secret_string_template=json.dumps({"username": f"ft-{env}-datawarehouse-master-user"}),
                exclude_punctuation=True,
                exclude_characters='"@/',
                include_space=False,
                generate_string_key="password"
            )
        )

        # Aurora PostgreSQL Database Cluster
        db_cluster = rds.DatabaseCluster(
            self,
            f"ft-{env}-aurora-db-cluster",
            vpc=boostrap_stack.decision_support_vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=boostrap_stack.decision_support_vpc.private_subnets
            ),
            credentials=rds.Credentials.from_secret(db_secret, username="masteruser"),
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_15_3 
            ),
            default_database_name="postgres",
            readers=[
                rds.ClusterInstance.serverless_v2(
                    f"ft-{env}-reader-instance-1", 
                    scale_with_writer=True,
                    enable_performance_insights=True
                ),
                rds.ClusterInstance.serverless_v2(
                    f"ft-{env}-reader-instance-2",
                    enable_performance_insights=True
                )
            ],
            writer=rds.ClusterInstance.serverless_v2(
                f"ft-{env}-writer-instance",
                enable_performance_insights=True
            ),
            serverless_v2_min_capacity=0.5,
            serverless_v2_max_capacity=2,
            security_groups=[rds_sg]
        )

        # Create IAM Role for the Bastion Host with Session Manager permissions
        bastion_role = iam.Role(
            self, "BastionHostRole",
            role_name=f"ft-{env}-bastion-host-session-manager-role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore")
            ]
        )

        # Bastion Host in Public Subnet for DB Access
        bastion_host = ec2.Instance(
            self, "BastionHost",
            instance_name=f"ft-{env}-bastion-host",
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.NANO),
            machine_image=ec2.MachineImage.latest_amazon_linux(),
            vpc=boostrap_stack.decision_support_vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_group=bastion_sg,
            role=bastion_role,
            key_name=f"ft-{env}-key"
        )

        # bastion_host = ec2.BastionHostLinux(
        #     self, "BastionHost",
        #     vpc=boostrap_stack.decision_support_vpc,
        #     subnet_selection=ec2.SubnetSelection(subnets=boostrap_stack.decision_support_vpc.public_subnets),  
        #     instance_name=f"ft-{env}-bastion-host",  
        #     role=bastion_role  # Attach the role for SSM access
        # )

        # # Output the necessary values
        # core.CfnOutput(self, "DBSecretArn", value=db_secret.secret_arn)
        # core.CfnOutput(self, "BastionHostPublicIp", value=bastion_host.instance_public_ip)
        # core.CfnOutput(self, "S3PrivateKeyLocation", value=f"s3://{bucket.bucket_name}/{s3_key_name}")



        '''
        # Create a new EC2 key name pair
        new_key_pair_name = f"ft-{env}-key-pair"
         
        new_key_pair = ec2.CfnKeyPair(
             self,
             "NewKeyNamePair",
             key_name=new_key_pair_name
	    )

        # add interface endpoint for secret manager
        datawarehouse_vpc.add_interface_endpoint(
            f"ft-{env}-secret-manager-endpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER
        )

        # create security group to allow other services to connect to RDS Proxy 
        rds_proxy_connection_security_group = ec2.SecurityGroup(
            self,
            f"ft-{env}-rds-proxy-connection-security-group",
            vpc=datawarehouse_vpc
        )

        # create security group to allow Proxy to connect to DB
        rds_db_connection_security_group = ec2.SecurityGroup(
            self,
            f"ft-{env}-db-connection-security-group",
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
        

        # create AWS Secret for database username
        database_username = "ft" + env + "dbuser"

        database_credential_secret = secrets.Secret(
            self,
            f"ft-{env}-database-credentials-secret",
            generate_secret_string=secrets.SecretStringGenerator(
                secret_string_template=json.dumps({"username": database_username}),
                exclude_punctuation=True,
                exclude_characters='"@/',
                include_space=False,
                generate_string_key="password"
            )
        )

        
        ssm.StringParameter(
            self,
            "DBCredentialsArn",
            parameter_name=f"ft-{env}-rds-credentials-arn",
            string_value=database_credential_secret.secret_arn
        )
        '''

        
        # # Create Aurora Serverless DB Cluster
        # db_cluster = rds.DatabaseCluster(
        #     self,
        #     f"ft-{env}-aurora-db-cluster",
        #     vpc=datawarehouse_vpc,
        #     vpc_subnets=ec2.SubnetSelection(
        #         subnets=private_subnets
        #     ),
        #     credentials=rds.Credentials.from_secret(database_credential_secret),
        #     engine=rds.DatabaseClusterEngine.aurora_postgres(
        #         version=rds.AuroraPostgresEngineVersion.VER_15_3 
        #     ),
        #     default_database_name="decision_support",
        #     readers=[
        #         rds.ClusterInstance.serverless_v2(
        #             f"ft-{env}-reader-instance-1", 
        #             scale_with_writer=True,
        #             enable_performance_insights=True
        #         ),
        #         rds.ClusterInstance.serverless_v2(
        #             f"ft-{env}-reader-instance-2",
        #             enable_performance_insights=True
        #         )
        #     ],
        #     writer=rds.ClusterInstance.serverless_v2(
        #         f"ft-{env}-writer-instance",
        #         enable_performance_insights=True
        #     ),
        #     serverless_v2_min_capacity=0.5,
        #     serverless_v2_max_capacity=8
        #     #security_groups=[rds_db_connection_security_group]
        # )
        
        
        # """
        # # ServerlessCluster creates an Aurora Serverless v1 Cluster!!!
        # serverless_cluster = rds.ServerlessCluster(
        #     self,
        #     f"ft-{env}-aurora-serverless-cluster",
        #     vpc=datawarehouse_vpc,
        #     credentials=rds.Credentials.from_secret(database_credential_secret),
        #     engine=rds.DatabaseClusterEngine.aurora_postgres(
        #         version=rds.AuroraPostgresEngineVersion.VER_15_3 
        #     ),
        #     scaling=rds.ServerlessScalingOptions(
        #         auto_pause=Duration.minutes(10),  # Auto-pause after 10 minutes
        #         min_capacity=rds.AuroraCapacityUnit.ACU_2,
        #         max_capacity=rds.AuroraCapacityUnit.ACU_8
        #     ),
        #     enable_data_api=True,  # Enable Data API for Aurora Serverless
        #     default_database_name="decision_support",
        # )
        # """

        #  # Define a security group for the RDS Proxy
        # rds_proxy_sg = ec2.SecurityGroup(
        #     self, 
        #     f"ft-{env}-rds-proxy-security-group",
        #     vpc=datawarehouse_vpc,
        #     description="Security group for RDS Proxy",
        #     allow_all_outbound=True
        # )

        # # Allow inbound traffic on the database port from your IP address
        # rds_proxy_sg.add_ingress_rule(
        #     # peer=ec2.Peer.ipv4(datawarehouse_vpc.vpc_cidr_block), # Allows access from within the VPC
        #     # RuntimeError: Error: Cannot perform this operation: 'vpcCidrBlock' was not supplied when creating this VPC
        #     peer=ec2.Peer.any_ipv4(),  # ZZZ - Wide open, need to figure out better security here
        #     connection=ec2.Port.tcp(5432)  # PostgreSQL port
        # )
        
        # # Create an RDS Proxy for the Aurora DB
        # rds_proxy = db_cluster.add_proxy(
        #     f"ft-{env}-rds-proxy",
        #     secrets=[database_credential_secret],
        #     vpc=datawarehouse_vpc,
        #     vpc_subnets=ec2.SubnetSelection(
        #         subnets=private_subnets
        #     ),
        #     security_groups=[rds_proxy_sg],
        #     iam_auth=True  # Enable IAM authentication
        # )

        # '''
        # rds_proxy = rds.DatabaseProxy(
        #     self, 
        #     f"ft-{env}-rds-database-proxy",
        #     proxy_target=rds.ProxyTarget.from_cluster(serverless_cluster),
        #     secrets=[database_credential_secret],
        #     vpc=datawarehouse_vpc,
        #     iam_auth=True,
        #     vpc_subnets=ec2.SubnetSelection(
        #         subnets=datawarehouse_vpc.private_subnets
        #     ),
        #     security_groups=[rds_proxy_sg]
        # )
        # '''

        # # Create a security group for the bastion host
        # bastion_sg = ec2.SecurityGroup(
        #     self, 
        #     f"ft-{env}-bastion-security-group",
        #     vpc=datawarehouse_vpc,
        #     description="Security group for Bastion Host",
        #     allow_all_outbound=True)
        
        # # Allow the bastion host to connect to the RDS Proxy on port 5432 explicitly
        # rds_proxy_sg.add_ingress_rule(
        #     peer=bastion_sg,
        #     connection=ec2.Port.tcp(5432),  # Explicitly allowing port 5432
        #     description="Allow Bastion Host to connect to RDS Proxy on port 5432"
        # )
        
        # # Allow inbound SSH access to Bastion
        # bastion_sg.add_ingress_rule(
        #     peer=ec2.Peer.any_ipv4(),  # ZZZ - Wide open, need to figure out better security here
        #     connection=ec2.Port.tcp(22)  # SSH port
        # )
        
        # '''
        # # Launch an EC2 instance as the bastion host
        # bastion_host = ec2.Instance(
        #     self, 
        #     f"ft-{env}-bastion-host",
        #     instance_type=ec2.InstanceType("t2.micro"),
        #     machine_image=ec2.MachineImage.latest_amazon_linux(),
        #     vpc=datawarehouse_vpc,
        #     # key_name=new_key_pair_name,
        #     security_group=bastion_sg,
        #     vpc_subnets=ec2.SubnetSelection(
        #         subnets=public_subnets
        #     )
        # )
        # '''

        # bastion_host = ec2.BastionHostLinux(
        #     self, 
        #     f"ft-{env}-bastion-host",
        #     vpc=datawarehouse_vpc,
        #     security_group=bastion_sg,
        #     subnet_selection=ec2.SubnetSelection(
        #         subnets=public_subnets
        #     )
        # )

        # # Use DESTROY in Dev environment only only
        # if (env=="dev"):
        #     db_cluster.removal_policy=RemovalPolicy.DESTROY 
        #     rds_proxy.apply_removal_policy(RemovalPolicy.DESTROY)

        # # Output the RDS Proxy endpoint
        # # CfnOutput(self, "RDSProxyEndpoint", value=db_cluster..endpoint)

        # # Output the Aurora Cluster endpoint
        # # CfnOutput(self, "AuroraClusterEndpoint", value=db_cluster.cluster_endpoint.hostname)
