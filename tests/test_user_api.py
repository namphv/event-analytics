import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.services.user_service import UserService
from app.routers.users import get_user_service
from tests.conftest import TEST_TABLE_NAME


@pytest.fixture
def client(dynamodb_resource):
    """Create test client with overridden dependencies"""

    def get_test_user_service():
        return UserService(dynamodb_resource, TEST_TABLE_NAME)

    app.dependency_overrides[get_user_service] = get_test_user_service

    with TestClient(app) as test_client:
        yield test_client

    # Clean up dependency overrides
    app.dependency_overrides = {}


@pytest.fixture
def valid_user_data():
    """Valid user data for API testing"""
    return {
        "firstName": "John",
        "lastName": "Doe",
        "phoneNumber": "+1234567890",
        "email": "john.doe@example.com",
        "avatar": "https://example.com/avatar.jpg",
        "gender": "Male",
        "jobTitle": "Software Engineer",
        "company": "Tech Corp",
        "city": "San Francisco",
        "state": "CA",
    }


def test_create_user_api_success(client, valid_user_data):
    """Test successful user creation via API"""
    response = client.post("/users/", json=valid_user_data)

    assert response.status_code == 201
    data = response.json()

    assert data["firstName"] == valid_user_data["firstName"]
    assert data["lastName"] == valid_user_data["lastName"]
    assert data["email"] == valid_user_data["email"]
    assert data["hostedEventCount"] == 0
    assert data["attendedEventCount"] == 0
    assert "id" in data
    assert len(data["id"]) > 0


def test_create_user_api_invalid_data(client):
    """Test user creation with invalid data via API"""
    invalid_data = {
        "firstName": "John",
        "lastName": "Doe",
        "phoneNumber": "+1234567890",
        "email": "invalid-email",  # Invalid email format
        "company": "Tech Corp",
    }

    response = client.post("/users/", json=invalid_data)
    assert response.status_code == 422  # Validation error


def test_filter_users_api_response(client, valid_user_data):
    """Test user filtering via API"""
    # First create some users
    user1_data = valid_user_data.copy()
    user1_data["email"] = "alice@techcorp.com"
    user1_data["firstName"] = "Alice"
    user1_data["lastName"] = "Smith"

    user2_data = valid_user_data.copy()
    user2_data["email"] = "bob@othercorp.com"
    user2_data["firstName"] = "Bob"
    user2_data["lastName"] = "Johnson"
    user2_data["company"] = "Other Corp"

    # Create users
    client.post("/users/", json=user1_data)
    client.post("/users/", json=user2_data)

    # Test filtering by company
    response = client.get("/users/?company=Tech Corp")
    assert response.status_code == 200

    data = response.json()
    assert data["count"] == 1
    assert data["hasMore"] is False
    assert len(data["users"]) == 1
    assert data["users"][0]["company"] == "Tech Corp"
    assert data["users"][0]["firstName"] == "Alice"


def test_filter_users_with_event_counts(client, valid_user_data):
    """Test filtering users by event counts via API"""
    # Create a user
    response = client.post("/users/", json=valid_user_data)
    assert response.status_code == 201

    # Test filtering by hosted event count (should return users with 0 hosted events)
    response = client.get("/users/?hostedEventCountMin=0&hostedEventCountMax=0")
    assert response.status_code == 200

    data = response.json()
    assert data["count"] >= 1
    assert all(user["hostedEventCount"] == 0 for user in data["users"])
