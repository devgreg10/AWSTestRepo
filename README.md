# First Tee Decision Support Repository
This repository will store all code related to the decision support system effort.

## Local development
When getting ready to push a branch and merge to develop be sure to run `cdk synth` and `pytest tests` to check for errors or failing tests.

## Git Flow
Developers should also build features on a new branch then open a pull request to develop to merge and the CI/CD pipeline to run. `dev` goes to `uat` and finally `uat` goes to `main`. Each PR will kick off the pipeline for that environment of the same name.

## Development Team
|Team Member|Contact|
|---|---|
|Harry Schuster|hschuster@captechconsulting.com|
|David Kearfott|dkearfott@captechconsulting.com|


## Documentation
Most of the documentation accompanying the gamification effort can be found in the [First Tee Confluence environment here](https://firsttee.atlassian.net/wiki/spaces/DSS/overview?homepageId=475824403).


# Decision Support Flow

## Ingestion Layer
### Salesforce
Data from Salesforce is ingested to AWS S3 as json files via AWS AppFlow . AppFlow definitions in CDK for Python can be very involved when ingesting many fields per Salesforce Entity.  The easiest way to set this up initially is to use the AWS Console to create the AppFlow flow manually, and then using the AWS CLI to describe the flow using a command like the following: 
>  aws appflow describe-flow --flow-name ft-dev-ingestion-layer-salesforce-contact --profile FT_DATALAKE_ALL-DataScience-211125359849 

This definition can be written to an output file by including `> (filename).json`, for example
> aws appflow describe-flow --flow-name ft-dev-ingestion-layer-salesforce-contact --profile FT_DATALAKE_ALL-DataScience-211125359849  > ft-dev-ingestion-layer-salesforce-contact.json

The definition that gets written is a CloudFormation definition and will need to be translated to CDK for python.  **ChatGPT** can can easily assist in generating python code from a CloudFormation defintion.

The goal is to pull **All** data on the initial AppFlow run, then schedule the flow to run at some frequencey (daily in prod; hourly in dev initially) to pull incremental data based on the **systemmodstamp** field in Salesforce.  This means that subsequent runs will pick up any Salesforce record whose systemmodstamp value has changed since the last time the flow ran.  The issue is that there is not a way to define the flow to trigger to accomplish what we need because the first time an incremental run is triggered, it only picks up the last 30 days of data and not All the data like we need. According to [this explanation and tip from AWS](https://docs.aws.amazon.com/pt_br/appflow/latest/userguide/flow-triggers.html), in order to achieve what we want, the AppFlow flow will initially be set up to be triggered **OnDemand** and then will have to be manually updated to run on an incremental schedule.

The AppFlow flows are defined in our IaC (Infrastructure as Code) to instruct AWS to make each file about 64MB.  AWS does its best to do this, but the files do all seem to be a little bigger.  If they are too big, they can cause the Load Layer Lambdas to timeout after Lambda's 15 min threshold.  To date, timeouts have not occurred if the files are about 64MB, but they do time out if they are closer to 128MB.

## Load Layer
The Decision Support Load Layer is comprised of State Machines that orchestrate Lambdas to read data from S3 into the Raw Data Zone as strings in Aurora PostgreSQL.  
### Database Access
All logic that interacts with the database is isolated to the **data_core_layer** helper classes.  The **data_core_layer** gets packaged up as a Lambda Layer and included with Lambda defintions to allow database authentication and access.

The AppFlow flow defintion from the Ingestion Layer above also contains a list of SourceFields that can be used along with the help of **ChatGPT** again to generate Database Helper classes.  Here's an excerpt from the flow definition containing the field names:
```
"tasks": [
        {
            "sourceFields": [
                "Id",
                "Chapter_Affiliation__c",
                "ChapterID_CONTACT__c",
                "CASESAFEID__c",
                "Contact_Type__c",
                "Age__c",
                "Ethnicity__c",
                "Gender__c",
                "Grade__c",
                "Participation_Status__c",
                "MailingPostalCode",
                "MailingStreet",
                "MailingCity",
                "MailingState",
                "School_Name__c",
                "School_Name_Other__c",
                "FirstName",
                "LastName",
                "Birthdate",
                "AccountId",
                "LastModifiedDate",
                "IsDeleted",
                "CreatedDate"
            ],
            "connectorOperator": {
                "Salesforce": "PROJECTION"
            },
            "taskType": "Filter",
            "taskProperties": {}
        },
        ......
```

Each entity has its own db_models and db_helper class.  The representation of the S3 data is the **source model**, while the representation of the database table is the **raw model**.

### Load Layer State Machines (aka Step Functions)

The State Machines that orchestrate the load layer kick off 2 lambdas: 1 to return a list of all files to process, and 1 to process 1 file at a time.  The State Machine batches the file names and is defined to kick off 5 Lambdas at a time, each processing a different file name.  

## Tranform Layer

The Transform Layer of Decision Support is comprised of additional state machines that kick off Lambdas to execute stored procedures to perform the following:
- move data from the Raw Zone to the Valid Zone (1 per data source)
- move data from the Valid Zone to the Refined Zone (1 for all data sources)
- perform historical metric calculations (1 for all data sources)

An explanation of our Data Zones (raw, valid, refined) can be found [on this Confluence Page](https://docs.aws.amazon.com/pt_br/appflow/latest/userguide/flow-triggers.html)