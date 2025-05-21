from aws_cdk import (
    aws_iam as iam,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_secretsmanager as secrets,
    aws_cloudwatch_actions as actions,
    aws_sns_subscriptions as subs,
    aws_s3 as s3,
    aws_logs as logs,
    aws_sns as sns,
    Duration,
    Stack,
    RemovalPolicy,
    Tags
)
from constructs import Construct
from dotenv import load_dotenv
from cdk_ec2_key_pair import KeyPair

import os
import json

from cloud.layer_1_bootstrap.ft_decision_support_bootstrap_stack import FtDecisionSupportBootstrapStack

class FtDecisionSupportPersistentStorageStack(Stack):

   def __init__(self, 
                scope: Construct, 
                id: str, 
                env: str,
                email_addresses_to_alert_on_error: str,
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
        DATA LAKE - S3 Bucket
        '''
        data_lake_bucket_name = os.getenv('data_lake_bucket_name')
        
        self.data_lake_bucket = s3.Bucket(self, 
            "FTDevDataLakeBucket",
            bucket_name=data_lake_bucket_name,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
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

        # Create an IAM Role for S3 operations (move and delete files)
        s3_role = iam.Role(
            self, "S3DataLakeRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com")
        )

        # Add the necessary permissions to the role for moving and deleting files/subfolders
        s3_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:ListBucket",       # List files and folders in the bucket
                    "s3:GetObject",         # Read objects (required for copying)
                    "s3:PutObject",         # Write objects (required for copying to the destination)
                    "s3:DeleteObject"       # Delete objects (to remove the source file and delete subfolders)
                ],
                resources=[
                    self.data_lake_bucket.bucket_arn,       # The bucket itself
                    f"{self.data_lake_bucket.bucket_arn}/*" # All objects within the bucket
                ]
            )
        )

        # Add a bucket policy to allow the S3OperationsRole to access objects
        self.data_lake_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[s3_role],
                actions=[
                    "s3:ListBucket",
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                resources=[self.data_lake_bucket.bucket_arn, f"{self.data_lake_bucket.bucket_arn}/*"]
            )
        )

        '''
        DATA WAREHOUSE - Creation
            EC2 Key Pair creation
        '''
        
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

        db_username = f"ft_{env}_data_warehouse_master_user"

        # Create a new Secret for the RDS Master user
        self.db_master_user_secret = secrets.Secret(
            self, 
            "DBMasterUserSecret",
            secret_name = f"ft-{env}-datawarehouse-db-master-user-secret",
            generate_secret_string=secrets.SecretStringGenerator(
                secret_string_template=json.dumps({"username": db_username}),
                exclude_punctuation=True,
                exclude_characters='"@/',
                include_space=False,
                generate_string_key="password"
            )
        )

        self.default_database_name = "postgres"

        # Aurora PostgreSQL Database Cluster
        self.data_warehouse_db_cluster = rds.DatabaseCluster(
            self,
            f"ft-{env}-data-warehouse-db-cluster",
            vpc=boostrap_stack.decision_support_vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=boostrap_stack.decision_support_vpc.private_subnets
            ),
            credentials=rds.Credentials.from_secret(self.db_master_user_secret, username=db_username),
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_15_3 
            ),
            default_database_name=self.default_database_name,
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
                enable_performance_insights=True,
                publicly_accessible=False
            ),
            serverless_v2_min_capacity=0.5,
            serverless_v2_max_capacity=2,
            security_groups=[rds_sg],
            storage_encrypted=True,
            backup=rds.BackupProps(
                retention=Duration.days(7), # Retain backups for 7 days
                preferred_window="03:00-04:00"  # Set backup window between 3 AM to 4 AM
            ),
            monitoring_interval=Duration.seconds(60),
            cloudwatch_logs_exports=["postgresql"], # postgresql (error logs) 
            cloudwatch_logs_retention=logs.RetentionDays.ONE_MONTH,
        )
        
        Tags.of(self.data_warehouse_db_cluster).add("ForceRecreateReaders", "v2"); 

        # Rotate the master user password every 30 days
        self.data_warehouse_db_cluster.add_rotation_single_user(
            automatically_after=Duration.days(30),
            vpc_subnets=ec2.SubnetSelection(
                subnets=boostrap_stack.decision_support_vpc.private_subnets
            ),
        )

        alarm_sns_topic = sns.Topic(
            self,
            id="ErrorSnsTopicForDb",
            topic_name = f"ft-{env}-db-error-alarm-sns-topic",
            display_name="Error alarm for DB concerns"
        )

        # comma-delimited string of email addresses
        email_addresses = [email.strip() for email in email_addresses_to_alert_on_error.split(",") ]
        for email in email_addresses:
            # Add subscription to the SNS topic (e.g., email notification)
            alarm_sns_topic.add_subscription(subs.EmailSubscription(email))

        # Add alarm for high CPU
        # 60%
        self.data_warehouse_db_cluster.metric_cpu_utilization(
            period=Duration.minutes(1)
        ).create_alarm(
            self,
            id="Database Cluster CPU 60 Percent",
            alarm_name=f"ft-{env}-primary-postgres-database-cluster-cpu-utilization-60-percent-alarm",
            alarm_description="If any RDS instance in the RDS Global Database Cluster's regional cluster has a CPU 60 percent or higher display as In alarm.",
            threshold=60,
            evaluation_periods=2,
            datapoints_to_alarm=2
        ).add_alarm_action(actions.SnsAction(topic=alarm_sns_topic))

        # 80%
        self.data_warehouse_db_cluster.metric_cpu_utilization(
            period=Duration.minutes(1)
        ).create_alarm(
            self,
            id="Database Cluster CPU 80 Percent",
            alarm_name=f"ft-{env}-primary-postgres-database-cluster-cpu-utilization-80-percent-alarm",
            alarm_description="If any RDS instance in the RDS Global Database Cluster's regional cluster has a CPU 80 percent or higher display as In alarm.",
            threshold=60,
            evaluation_periods=2,
            datapoints_to_alarm=2
        ).add_alarm_action(actions.SnsAction(topic=alarm_sns_topic))

        # Deadlocks
        self.data_warehouse_db_cluster.metric_deadlocks(
            period=Duration.minutes(1)
        ).create_alarm(
            self,
            id="Database Cluster Deadlocks equal to or greater than 1",
            alarm_name=f"ft-{env}-primary-postgres-database-cluster-deadlocks-one-alarm",
            alarm_description="If the RDS Database Cluster has one or more deadlocks display as In alarm.",
            threshold=1,
            evaluation_periods=2,
            datapoints_to_alarm=2
        ).add_alarm_action(actions.SnsAction(topic=alarm_sns_topic))



        # Create another Secret for an Admin User that can be communicated to developers
        # self.data_warehouse_db_cluster.add_rotation_multi_user(
        #     id = "AdminUser",
        #     secret = rds.DatabaseSecret(
        #         self, 
        #         "DBAdminUserSecret",
        #         secret_name = f"ft-{env}-datawarehouse-db-admin-user-secret",
        #         username=f"ft_{env}_data_warehouse_admin_user",
        #         master_secret=self.db_master_user_secret,
        #         dbname=default_database_name
        #     )
        # )

        # Create another Secret for a Servie Account User to be used by our Lambdas
        # self.data_warehouse_db_cluster.add_rotation_multi_user(
        #     id = "LambdaUser",
        #     secret = rds.DatabaseSecret(
        #         self, 
        #         "DBLambdaUserSecret",
        #         secret_name = f"ft-{env}-datawarehouse-db-lambda-user-secret",
        #         username=f"ft_{env}_data_warehouse__lambdauser",
        #         master_secret=self.db_master_user_secret,
        #         dbname=default_database_name
        #     )
        # )

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




