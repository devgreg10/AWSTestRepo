#!/usr/bin/env python3
import os

import aws_cdk as cdk

from iac_code.first_tee_decision_support_stack import FirstTeeDecisionSupportStack

from dotenv import load_dotenv

load_dotenv()
env = os.environ.get('env')

app = cdk.App()

FirstTeeDecisionSupportStack(app, "FirstTee-CDK-{0}".format(env), env)


cdk.Tags.of(app).add("Project", "First Tee Decision Support")
cdk.Tags.of(app).add("Deployment", "CDK")
cdk.Tags.of(app).add("Repo", "First_Tee_Decision_Support")

app.synth()