import json
import boto3
import psycopg2
from botocore.exceptions import ClientError

def get_secret(secret_arn):
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response['SecretString'])
        return secret
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

    # Connect to DB
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

        for user in [bi_user_secret, dev_user_secret]:
            username = user["username"]
            password = user["password"]

            print(f"Creating or updating user: {username}")

            # Create or update the user
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT FROM pg_catalog.pg_roles WHERE rolname = '{username}'
                    ) THEN
                        CREATE ROLE {username} WITH LOGIN PASSWORD %s;
                    ELSE
                        ALTER ROLE {username} WITH PASSWORD %s;
                    END IF;
                END
                $$;
            """, (password, password))

            if "bi" in username:
                print(f"Granting read-only permissions to {username}")
                # Grant connect on the database
                cur.execute(f"GRANT CONNECT ON DATABASE {dbname} TO {username};")

                # Grant usage and SELECT on all schemas and tables
                cur.execute(f"""
                    DO $$
                    DECLARE
                        r RECORD;
                    BEGIN
                        FOR r IN SELECT nspname FROM pg_namespace WHERE nspname NOT IN ('pg_catalog', 'information_schema') LOOP
                            EXECUTE format('GRANT USAGE ON SCHEMA %I TO {username}', r.nspname);
                            EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO {username}', r.nspname);
                            EXECUTE format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA %I TO {username}', r.nspname);
                        END LOOP;
                    END
                    $$;
                """)

            elif "dev" in username:
                print(f"Granting full privileges to {username}")
                cur.execute(f"GRANT rds_superuser TO {username};")


            # Grant full privileges
            cur.execute(f"GRANT rds_superuser TO {username};")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error connecting to DB or creating users: {e}")
        raise

    return {
        "Status": "SUCCESS",
        "Reason": "DB users created successfully",
        "PhysicalResourceId": f"{host}-db-user-setup",
    }
