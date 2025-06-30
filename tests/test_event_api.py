import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.schemas.user import UserCreate
from app.services.user_service import UserService
from app.services.event_service import EventService
from app.routers.events import get_event_service
from tests.conftest import TEST_TABLE_NAME


@pytest.fixture
def client(dynamodb_resource):
    """Create test client with overridden dependencies"""

    def get_test_event_service():
        return EventService(dynamodb_resource, TEST_TABLE_NAME)

    app.dependency_overrides[get_event_service] = get_test_event_service

    with TestClient(app) as test_client:
        yield test_client

    # Clean up dependency overrides
    app.dependency_overrides = {}


@pytest.fixture
def user_service(dynamodb_resource):
    """Create UserService instance with test table"""
    return UserService(dynamodb_resource, TEST_TABLE_NAME)


@pytest.fixture
def sample_users(user_service):
    """Create sample users for testing"""
    users = []

    user1 = user_service.create_user(
        UserCreate(
            firstName="John",
            lastName="Host",
            phoneNumber="+1111111111",
            email="john@example.com",
            company="Tech Corp",
        )
    )
    users.append(user1)

    user2 = user_service.create_user(
        UserCreate(
            firstName="Jane",
            lastName="Attendee",
            phoneNumber="+2222222222",
            email="jane@example.com",
            company="Design Co",
        )
    )
    users.append(user2)

    return users


@pytest.fixture
def valid_event_data():
    """Valid event data for API testing"""
    return {
        "slug": "tech-meetup-2024",
        "title": "Tech Meetup 2024",
        "description": "A great tech meetup",
        "startAt": "2024-03-15T18:00:00Z",
        "endAt": "2024-03-15T20:00:00Z",
        "venue": "Tech Hub",
        "maxCapacity": 100,
        "owner": "owner-user-id",
    }


def test_create_event_api_success(client, valid_event_data):
    """Test successful event creation via API"""
    response = client.post("/events/", json=valid_event_data)

    assert response.status_code == 201
    data = response.json()

    assert data["slug"] == valid_event_data["slug"]
    assert data["title"] == valid_event_data["title"]
    assert data["description"] == valid_event_data["description"]
    assert data["venue"] == valid_event_data["venue"]
    assert data["maxCapacity"] == valid_event_data["maxCapacity"]
    assert data["owner"] == valid_event_data["owner"]
    assert data["attendeeCount"] == 0
    assert "id" in data
    assert len(data["id"]) > 0


def test_create_event_with_participants_api(client, sample_users):
    """Test event creation with participants via API"""
    event_data = {
        "slug": "team-event-2024",
        "title": "Team Event 2024",
        "description": "Team building event",
        "startAt": "2024-04-20T10:00:00Z",
        "endAt": "2024-04-20T16:00:00Z",
        "venue": "Conference Center",
        "maxCapacity": 50,
        "owner": sample_users[0].id,
        "hostIds": [sample_users[0].id],
        "attendeeIds": [sample_users[1].id],
    }

    response = client.post("/events/", json=event_data)

    assert response.status_code == 201
    data = response.json()

    assert data["title"] == event_data["title"]
    assert data["attendeeCount"] == 1  # One attendee
    assert data["owner"] == sample_users[0].id


def test_create_event_api_invalid_data(client):
    """Test event creation with invalid data via API"""
    invalid_data = {
        "slug": "invalid-event",
        "title": "Invalid Event",
        "description": "Missing required fields",
        # Missing startAt, endAt, venue, maxCapacity, owner
    }

    response = client.post("/events/", json=invalid_data)
    assert response.status_code == 422  # Validation error


def test_create_event_with_nonexistent_users_api(client):
    """Test event creation with nonexistent users via API"""
    event_data = {
        "slug": "failing-event",
        "title": "Failing Event",
        "description": "This should fail",
        "startAt": "2024-05-01T09:00:00Z",
        "endAt": "2024-05-01T17:00:00Z",
        "venue": "Nowhere",
        "maxCapacity": 10,
        "owner": "nonexistent-user-id",
        "hostIds": ["nonexistent-host-id"],
        "attendeeIds": ["nonexistent-attendee-id"],
    }

    response = client.post("/events/", json=event_data)
    assert response.status_code == 500  # Should fail due to nonexistent users
