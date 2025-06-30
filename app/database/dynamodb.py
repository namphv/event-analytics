import boto3
from botocore.exceptions import NoCredentialsError


def get_db_connection():
    try:
        dynamodb = boto3.resource("dynamodb", endpoint_url="http://dynamodb:8000")
        return dynamodb
    except NoCredentialsError:
        print("Credentials not available.")
        return None
