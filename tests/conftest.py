import pytest
import os
import boto3
import sys
import time
from scripts.init_dynamodb import create_table_if_not_exists, delete_table

# Add the project root to the Python path
sys.path.insert(0, "/app")

TEST_TABLE_NAME = "CommunityApp_Test"


@pytest.fixture(scope="session")
def dynamodb_table():
    """Create test DynamoDB table for the session"""
    # Set environment variables for test
    os.environ["DYNAMODB_ENDPOINT"] = "http://dynamodb-local:8000"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "fake"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "fake"

    # Wait for DynamoDB to be ready
    time.sleep(2)

    # Create test table
    table = create_table_if_not_exists(TEST_TABLE_NAME)

    yield table

    # Cleanup: Delete test table
    delete_table(TEST_TABLE_NAME)


@pytest.fixture
def dynamodb_resource(dynamodb_table):
    """Get DynamoDB resource for tests"""
    resource = boto3.resource(
        "dynamodb",
        endpoint_url=os.getenv("DYNAMODB_ENDPOINT", "http://dynamodb-local:8000"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "fake"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "fake"),
    )

    # Clean up the table before each test
    table = resource.Table(TEST_TABLE_NAME)

    # Scan and delete all items
    response = table.scan()
    items = response.get("Items", [])

    for item in items:
        table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})

    return resource
