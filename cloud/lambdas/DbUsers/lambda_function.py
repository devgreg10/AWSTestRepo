import os
import json
import boto3
import psycopg2
from botocore.exceptions import ClientError

def get_secret(secret_arn):
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_arn)
        return json.loads(response['SecretString'])
    except ClientError as e:
        print(f"Error fetching secret {secret_arn}: {e}")
        raise

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    request_type = event.get("RequestType")

    if request_type == "Delete":
        print("Delete request, skipping user creation.")
        return {"Status": "SUCCESS"}

    # Load parameters
    props = event["ResourceProperties"]
    host = props["DBHost"]
    port = int(props.get("DBPort", 5432))
    dbname = props["DBName"]
    master_secret_arn = props["MasterSecretArn"]
    bi_user_secret_arn = props["BIUserSecretArn"]
    dev_user_secret_arn = props["DevUserSecretArn"]

    # Fetch secrets
    master_secret = get_secret(master_secret_arn)
    bi_user_secret = get_secret(bi_user_secret_arn)
    dev_user_secret = get_secret(dev_user_secret_arn)

    master_user = master_secret["username"]
    master_password = master_secret["password"]

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=master_user,
            password=master_password
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Create or update BI Dashboard User
        bi_username = bi_user_secret["username"]
        bi_password = bi_user_secret["password"]

        print(f"Creating or updating BI user: {bi_username}")
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT FROM pg_catalog.pg_roles WHERE rolname = '{bi_username}'
                ) THEN
                    CREATE ROLE {bi_username} WITH LOGIN PASSWORD %s;
                ELSE
                    ALTER ROLE {bi_username} WITH PASSWORD %s;
                END IF;
            END
            $$;
        """, (bi_password, bi_password))

        # Grant read-only role
        print(f"Granting rds_read_only to {bi_username}")
        cur.execute(f"GRANT rds_read_only TO {bi_username};")

        # Create or update Dev User
        dev_username = dev_user_secret["username"]
        dev_password = dev_user_secret["password"]

        print(f"Creating or updating Dev user: {dev_username}")
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT FROM pg_catalog.pg_roles WHERE rolname = '{dev_username}'
                ) THEN
                    CREATE ROLE {dev_username} WITH LOGIN PASSWORD %s;
                ELSE
                    ALTER ROLE {dev_username} WITH PASSWORD %s;
                END IF;
            END
            $$;
        """, (dev_password, dev_password))

        print(f"Granting rds_superuser to {dev_username}")
        cur.execute(f"GRANT rds_superuser TO {dev_username};")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error creating users: {e}")
        raise

    return {
        "Status": "SUCCESS",
        "Reason": "DB users created successfully",
        "PhysicalResourceId": f"{host}-db-user-setup"
    }
