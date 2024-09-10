from aws_cdk import (
    aws_secretsmanager as secretsmanager,
    Stack,
    CfnOutput,
    SecretValue
)

from constructs import Construct

class FtCreateSecretsStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Store the known Aurora PostgreSQL credentials in AWS Secrets Manager
        self.db_secret = secretsmanager.Secret(self, "AuroraDBSecret",
            description="Aurora PostgreSQL DB credentials",
            secret_name="ft-aurora-postgres-connection-secret",
            secret_string_value=SecretValue.plain_text(
                '{"username":"ftdevpublicdbuser","password":"tempdbpassword_kirkcousinsqbatlantafalc0ns","dbname":"postgres","host":"firsttee-cdk-public-data-ftdevpublicauroradbclust-j3otj0e0ak2o.cluster-c5qwcaqekjc2.us-east-1.rds.amazonaws.com"}'
            )
        )
        
         # Log the secret ARN
        CfnOutput(self, "SecretArnOutput",
            value=self.db_secret.secret_arn,
            description="The ARN of the Aurora PostgreSQL Secret"
        )