from aws_cdk import (
    Duration,
    Stack,
    aws_sqs as sqs
)
from constructs import Construct
from dotenv import load_dotenv
import os

load_dotenv()


class FirstTeeDecisionSupportStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, env: str, **kwargs) -> None:
        if (env == None):
            env = 'no_env'

        super().__init__(scope, construct_id, **kwargs)

        # -------------------------------------------
        # SQS QUEUES
        # -------------------------------------------       

        test_queue = sqs.Queue(
            self, "testQueue",
            visibility_timeout=Duration.seconds(300),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            queue_name= "FirstTee-CDK-" + env + "-" + "test_queue"
        )