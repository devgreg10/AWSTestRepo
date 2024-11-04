from typing import List
from aws_cdk import (
    aws_appflow as appflow,
)
from constructs import Construct

class FtSalesforceEarnedBadgesAppFlowTasks(Construct):
    def __init__(self, scope: Construct, id: str) -> None:
        super().__init__(scope, id)



    def get_tasks(self)-> List[appflow.CfnFlow.TaskProperty]:
        tasks: List[appflow.CfnFlow.TaskProperty] = [
            appflow.CfnFlow.TaskProperty(
                source_fields=[
                "Id",
                "IsDeleted",
                "Name",
                "CreatedDate",
                "CreatedById",
                "LastModifiedDate",
                "LastModifiedById",
                "SystemModstamp",
                "Contact__c",
                "Badge__c",
                "Id__c",
                "Date_Earned__c",
                "Listing_Session__c",
                "Pending_AWS_Callout__c",
                "Points__c",
                "Source_System__c"
        ],

        connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(
                    salesforce="PROJECTION"
                ),
                task_type="Filter",
                task_properties=[]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["Id"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="Id",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="id"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="id")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["IsDeleted"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="IsDeleted",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="boolean"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="boolean")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["Name"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="Name",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="string"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="string")
                ]
                    ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["CreatedDate"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="CreatedDate",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="datetime"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="datetime")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["CreatedById"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="CreatedById",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="reference"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="reference")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["LastModifiedDate"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="LastModifiedDate",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="datetime"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="datetime")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["LastModifiedById"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="LastModifiedById",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="reference"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="reference")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["SystemModstamp"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="SystemModstamp",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="datetime"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="datetime")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["Contact__c"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="Contact__c",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="reference"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="reference")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["Badge__c"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="Badge__c",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="reference"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="reference")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["Id__c"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="Id__c",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="string"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="string")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["Date_Earned__c"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="Date_Earned__c",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="datetime"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="datetime")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["Listing_Session__c"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="Listing_Session__c",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="reference"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="reference")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["Pending_AWS_Callout__c"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="Pending_AWS_Callout__c",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="boolean"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="boolean")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["Points__c"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="Points__c",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="double"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="double")
                ]
            ),
            appflow.CfnFlow.TaskProperty(
                source_fields=["Source_System__c"],
                connector_operator=appflow.CfnFlow.ConnectorOperatorProperty(salesforce="NO_OP"),
                destination_field="Source_System__c",
                task_type="Map",
                task_properties=[
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="DESTINATION_DATA_TYPE", value="string"),
                    appflow.CfnFlow.TaskPropertiesObjectProperty(key="SOURCE_DATA_TYPE", value="string")
                ]
            )
        ]

        return tasks







