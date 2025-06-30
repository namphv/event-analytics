import boto3
from botocore.exceptions import ClientError
import time
import os


def create_table_if_not_exists(table_name="CommunityApp"):
    """Create DynamoDB table with GSIs if it doesn't exist"""

    dynamodb = boto3.resource(
        "dynamodb",
        endpoint_url=os.getenv("DYNAMODB_ENDPOINT", "http://localhost:8000"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "fake"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "fake"),
    )

    try:
        # Check if table exists
        table = dynamodb.Table(table_name)
        table.table_status
        print(f"Table {table_name} already exists")
        return table
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    # Create table with GSIs
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI_ByCompany_PK", "AttributeType": "S"},
            {"AttributeName": "GSI_ByCompany_SK", "AttributeType": "S"},
            {"AttributeName": "GSI_ByJobTitle_PK", "AttributeType": "S"},
            {"AttributeName": "GSI_ByJobTitle_SK", "AttributeType": "S"},
            {"AttributeName": "GSI_ByLocation_PK", "AttributeType": "S"},
            {"AttributeName": "GSI_ByLocation_SK", "AttributeType": "S"},
            {"AttributeName": "GSI_UsersByHostedCount_PK", "AttributeType": "S"},
            {"AttributeName": "GSI_UsersByHostedCount_SK", "AttributeType": "S"},
            {"AttributeName": "GSI_UsersByAttendedCount_PK", "AttributeType": "S"},
            {"AttributeName": "GSI_UsersByAttendedCount_SK", "AttributeType": "S"},
            {"AttributeName": "GSI_EventAttendees_PK", "AttributeType": "S"},
            {"AttributeName": "GSI_EventAttendees_SK", "AttributeType": "S"},
            {"AttributeName": "GSI_UsersByActivity_PK", "AttributeType": "S"},
            {"AttributeName": "GSI_UsersByActivity_SK", "AttributeType": "S"},
            {"AttributeName": "GSI_EventsByDate_PK", "AttributeType": "S"},
            {"AttributeName": "GSI_EventsByDate_SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "GSI_ByCompany",
                "KeySchema": [
                    {"AttributeName": "GSI_ByCompany_PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI_ByCompany_SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "GSI_ByJobTitle",
                "KeySchema": [
                    {"AttributeName": "GSI_ByJobTitle_PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI_ByJobTitle_SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "GSI_ByLocation",
                "KeySchema": [
                    {"AttributeName": "GSI_ByLocation_PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI_ByLocation_SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "GSI_UsersByHostedCount",
                "KeySchema": [
                    {"AttributeName": "GSI_UsersByHostedCount_PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI_UsersByHostedCount_SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "GSI_UsersByAttendedCount",
                "KeySchema": [
                    {"AttributeName": "GSI_UsersByAttendedCount_PK", "KeyType": "HASH"},
                    {
                        "AttributeName": "GSI_UsersByAttendedCount_SK",
                        "KeyType": "RANGE",
                    },
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "GSI_EventAttendees",
                "KeySchema": [
                    {"AttributeName": "GSI_EventAttendees_PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI_EventAttendees_SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "GSI_UsersByActivity",
                "KeySchema": [
                    {"AttributeName": "GSI_UsersByActivity_PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI_UsersByActivity_SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "GSI_EventsByDate",
                "KeySchema": [
                    {"AttributeName": "GSI_EventsByDate_PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI_EventsByDate_SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    )

    # Wait for table to be ready
    print(f"Creating table {table_name}...")
    table.wait_until_exists()

    # Wait for GSIs to be active
    print("Waiting for GSIs to be active...")
    while True:
        table.reload()
        gsi_statuses = [gsi["IndexStatus"] for gsi in table.global_secondary_indexes]
        if all(status == "ACTIVE" for status in gsi_statuses):
            break
        time.sleep(1)

    print(f"Table {table_name} created successfully")
    return table


def delete_table(table_name="CommunityApp"):
    """Delete DynamoDB table"""
    dynamodb = boto3.resource(
        "dynamodb",
        endpoint_url=os.getenv("DYNAMODB_ENDPOINT", "http://localhost:8000"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "fake"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "fake"),
    )

    try:
        table = dynamodb.Table(table_name)
        table.delete()
        table.wait_until_not_exists()
        print(f"Table {table_name} deleted successfully")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        print(f"Table {table_name} does not exist")


if __name__ == "__main__":
    create_table_if_not_exists()
