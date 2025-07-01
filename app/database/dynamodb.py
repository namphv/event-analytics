import boto3
import os
from botocore.exceptions import NoCredentialsError


def get_db_connection():
    try:
        dynamodb = boto3.resource(
            "dynamodb",
            endpoint_url=os.getenv(
                "DYNAMODB_ENDPOINT_URL", "http://dynamodb-local:8000"
            ),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "fake"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "fake"),
        )
        return dynamodb
    except NoCredentialsError:
        print("Credentials not available.")
        return None
