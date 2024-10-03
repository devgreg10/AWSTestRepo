from aws_cdk import (
    aws_lambda as lambda_,
    aws_sns as sns,
    aws_events as events,
    aws_stepfunctions as sfn,    
    aws_logs as logs,
    aws_stepfunctions_tasks as tasks,
    aws_events_targets as targets,
    aws_sns as sns,
    Duration,
    RemovalPolicy
)

from cloud.shared.state_machine_alarm_generator import StateMachineAlarmGenerator

class ExecuteSingleDbFunctionStateMachineGenerator:
    def __init__(self, scope, 
                 lambda_execute_db_function: lambda_.Function,
                 id_prefix: str, 
                 db_schema: str,
                 db_function: str,
                 db_secret_arn: str,
                 region: str,
                 cron_schedule: events.Schedule,
                 sns_topic: sns.Topic):
        """
        Reusable class to create a CloudWatch alarm for a given metric.

        :param scope: The CDK scope (Stack or Construct) where this alarm will be defined.
        :param lambda_execute_db_function: The Lambda Function used to execute the db function
        :param id_prefix: A prefix to create unique identifier for the alarm resource.
        :param db_schema: The DB schema of the DB function to call
        :param db_function: The DB function to call
        :param db_secret_arn: The ARN of an Secret storing the DB credentials
        :param region: The default region to deploy
        :param cron_schedule: The schedule to trigger the State Machine
        :param sns_topic: The SNS topic for alarms and monitoring
        """
        self.scope = scope
        self.lambda_execute_db_function = lambda_execute_db_function
        self.id_prefix = id_prefix 
        self.db_schema = db_schema
        self.db_function = db_function
        self.db_secret_arn = db_secret_arn
        self.region = region
        self.cron_schedule = cron_schedule
        self.sns_topic = sns_topic

        # Create the State Machine based on the passed parameters
        self.create_state_machine()

    def create_state_machine(self):
         
        # Create Task for Simple State Machine
    
        # Call stored procedure passed in
        db_function_task = tasks.LambdaInvoke(
            scope=self.scope,
            id=f"FtExecute{self.id_prefix}Task",
            lambda_function=self.lambda_execute_db_function,
            payload=sfn.TaskInput.from_object({
                "db_function": self.db_function,
                "db_schema": self.db_schema,
                "secret_arn": self.db_secret_arn,
                "region": self.region
            }),
            result_path="$.db_function_result"
        ).add_retry(
            max_attempts=3,
            interval=Duration.seconds(5),
            backoff_rate=2.0
        ).add_catch(
            sfn.Fail(self.scope, f"{self.id_prefix}failed", error=f"{self.db_schema}.{self.db_function} failed", cause=f"{self.db_schema}.{self.db_function} failed"),
            errors=["States.ALL"]
        )

        # Define the state machine workflow
        execute_db_function_state_machine_definition = db_function_task

        execute_db_function_log_group = logs.LogGroup(
            scope=self.scope, 
            id=f"FtExecute{self.id_prefix}LogGroup",
            log_group_name=f"{self.id_prefix}-log-group",
            retention=logs.RetentionDays.ONE_WEEK,  # Retain logs for 1 week
            removal_policy=RemovalPolicy.DESTROY
        )

        # Create the state machine
        execute_db_function_state_machine = sfn.StateMachine(
            scope=self.scope, 
            id=f"FtExecute{self.id_prefix}StateMachine",
            state_machine_name=f"{self.id_prefix}-state-machine",
            definition=execute_db_function_state_machine_definition,
            timeout=Duration.minutes(60),
            logs=sfn.LogOptions(
                destination=execute_db_function_log_group,
                level=sfn.LogLevel.ALL  # Log all events 
            )
        )

        # ___________________________________
        # TRIGGER STATE MACHINE ON A SCHEDULE
        # ___________________________________

        # Create a cron rule for to trigger state machine
        rule = events.Rule(
            scope=self.scope, 
            id=f"FtExecute{self.id_prefix}CronRule",
            schedule=self.cron_schedule,
            rule_name= f"{self.id_prefix}-cron-rule"
        )

        # Add the state machine as a target for the rule
        rule.add_target(targets.SfnStateMachine(execute_db_function_state_machine))

        # _________________________________________
        # MONITORING - ALARM UPON ERROR or TIME OUT
        # _________________________________________
        
        StateMachineAlarmGenerator(
            scope=self.scope,
            id_prefix=self.id_prefix,
            alarm_name_prefix=f"{self.id_prefix}-alarm",
            log_group=execute_db_function_log_group,
            sns_topic=self.sns_topic,
            state_machine=execute_db_function_state_machine
        )
