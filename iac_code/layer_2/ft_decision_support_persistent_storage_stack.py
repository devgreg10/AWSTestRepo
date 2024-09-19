from aws_cdk import (
    aws_iam as iam,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_secretsmanager as secrets,
    aws_s3 as s3,
    aws_cloudfront as cloudfront,
    Stack,
    RemovalPolicy
)

from constructs import Construct
from dotenv import load_dotenv
from cdk_ec2_key_pair import KeyPair

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
        # '''
        # # Create S3 bucket to store EC2 name pair
        # bastion_key_bucket = s3.Bucket(
        #     self, 
        #     "BastionKeyBucket", 
        #     bucket_name=f"ft-{env}-bastion-key-bucket",
        #     removal_policy=RemovalPolicy.DESTROY)
        
        # # Create Iam Policy to allow users to access and download certificates
        # certificate_access_policy = iam.PolicyStatement(
        #     effect=iam.Effect.ALLOW,
        #     principals=[iam.AccountRootPrincipal()],  # Allows any user from the account
        #     actions=[
        #         "s3:ListBucket",    # List objects in the bucket
        #         "s3:GetObject",     # Get objects (needed for move)
        #         "s3:PutObject",     # Put objects (needed for move)
        #         "s3:DeleteObject"   # Delete objects (needed for move)
        #     ],
        #     resources=[
        #         bastion_key_bucket.bucket_arn,        # Required for s3:ListBucket
        #         f"{bastion_key_bucket.bucket_arn}/*"  # All objects in the bucket
        #     ]
        # )

        # # Attach the move files policy to the bucket
        # bastion_key_bucket.add_to_resource_policy(certificate_access_policy)
        
        # Create the key pair
        bastion_key_pair = KeyPair(
            self, 
            "BastionKeyPair",
            key_pair_name=f"ft-{env}-bastion-key" # private key is stored in a Secret prefixed with "ec2-ssh-key/", more info here: https://www.npmjs.com/package/cdk-ec2-key-pair
        )

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

        db_username = f"ft-{env}-datawarehouse-master-user"

        # Create a new Secret for the RDS Master user
        db_secret = secrets.Secret(
            self, 
            "DBSecret",
            secret_name = f"ft-{env}-datawarehouse-db-master-user-secret",
            generate_secret_string=secrets.SecretStringGenerator(
                secret_string_template=json.dumps({"username": db_username}),
                exclude_punctuation=True,
                exclude_characters='"@/',
                include_space=False,
                generate_string_key="password"
            )
        )

        # Aurora PostgreSQL Database Cluster
        self.data_warehouse_db_cluster = rds.DatabaseCluster(
            self,
            f"ft-{env}-data-warehouse-db-cluster",
            vpc=boostrap_stack.decision_support_vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=boostrap_stack.decision_support_vpc.private_subnets
            ),
            credentials=rds.Credentials.from_secret(db_secret, username=db_username),
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
            key_name=bastion_key_pair.key_pair_name
        )

       
