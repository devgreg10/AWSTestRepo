from aws_cdk import (
    aws_cloudwatch as cloudwatch,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_cloudwatch_actions as cw_actions,
    aws_stepfunctions as sfn,    
    aws_logs as logs,
    Duration
)

class StateMachineAlarmGenerator:
    def __init__(self, scope, 
                 id_prefix: str, 
                 alarm_name_prefix: str,
                 state_machine: sfn.StateMachine, 
                 sns_topic: sns.Topic,
                 log_group: logs.LogGroup):
        """
        Reusable class to create a CloudWatch alarm for a given metric.

        :param scope: The CDK scope (Stack or Construct) where this alarm will be defined.
        :param id_prefix: A prefix to create unique identifier for the alarm resource.
        :param alarm_name_prefix: Prefix to use for all alarms generated
        :param state_machine: The state machine to create alerts/alarms
        :param sns_topic_arn: The ARN of an SNS topic for alarm notifications.
        :param log_group: Log group of the state machine
        """
        self.scope = scope
        self.id_prefix = id_prefix 
        self.alarm_name_prefix = alarm_name_prefix 
        self.state_machine = state_machine
        self.sns_topic = sns_topic
        self.log_group = log_group

        # Create the alarm based on the passed parameters
        self.create_alarms()

    def create_alarms(self):
         
        failed_executions_metric = self.state_machine.metric("ExecutionsFailed")

        failed_executions_alarm = cloudwatch.Alarm(
            self.scope, f"{self.id_prefix}FailedExecutionsAlarm",
            alarm_name=f"{self.alarm_name_prefix}-executions-failed",
            metric=failed_executions_metric,
            threshold=1,  # Alarm if more than 1 failure occurs
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )

        # Add SNS action to the alarm
        failed_executions_alarm.add_alarm_action(cw_actions.SnsAction(self.sns_topic))

        # Similarly, create alarm ExecutionsTimedOut
        timed_out_executions_metric = self.state_machine.metric("ExecutionsTimedOut")

        timed_out_executions_alarm = cloudwatch.Alarm(
            self.scope, f"{self.id_prefix}TimedOutExecutionsAlarm",
            alarm_name=f"{self.alarm_name_prefix}-timeout-exception",
            metric=timed_out_executions_metric,
            threshold=1,  # Alarm if any execution times out
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            datapoints_to_alarm=1
        )

        # Attach SNS topic to this alarm as well
        timed_out_executions_alarm.add_alarm_action(cw_actions.SnsAction(self.sns_topic))

        combined_metric_filter = logs.MetricFilter(
            self.scope, f"{self.id_prefix}CombinedErrorMetricFilter",
            log_group=self.log_group,
            metric_namespace="StepFunctionErrors",
            metric_name="CombinedErrors",
            filter_pattern=logs.FilterPattern.any_term(
                "ExecutionsFailed", 
                "ExecutionsTimedOut", 
                '"Error": "DbException"',
                "DB Error",
                "Unhandled exception",
                "[ERROR]"
            ),
            metric_value="1"  # Each occurrence of any of these terms increments the metric by 1
        )

        combined_error_alarm = cloudwatch.Alarm(
            self.scope, f"{self.id_prefix}CombinedErrorAlarm",
            metric=cloudwatch.Metric(
                namespace="StepFunctionErrors",
                metric_name="CombinedErrors",
                period=Duration.minutes(1),
                statistic="Sum"
            ),
            alarm_name=f"{self.id_prefix}-combined-log-error-alarm",
            threshold=1,  # Alarm if any error (ExecutionFailed, TimedOut, or DbException) occurs
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
        )

        combined_error_alarm.add_alarm_action(cw_actions.SnsAction(self.sns_topic))
        
