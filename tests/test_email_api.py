import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.schemas.user import UserCreate
from app.services.user_service import UserService
from app.services.email_service import EmailService
from app.routers.emails import get_email_service
from tests.conftest import TEST_TABLE_NAME


@pytest.fixture
def client(dynamodb_resource):
    """Create test client with overridden dependencies"""

    def get_test_email_service():
        return EmailService(dynamodb_resource, TEST_TABLE_NAME)

    app.dependency_overrides[get_email_service] = get_test_email_service

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
            lastName="Developer",
            phoneNumber="+1111111111",
            email="john@techcorp.com",
            company="Tech Corp",
            jobTitle="Developer",
        )
    )
    users.append(user1)

    user2 = user_service.create_user(
        UserCreate(
            firstName="Jane",
            lastName="Designer",
            phoneNumber="+2222222222",
            email="jane@techcorp.com",
            company="Tech Corp",
            jobTitle="Designer",
        )
    )
    users.append(user2)

    user3 = user_service.create_user(
        UserCreate(
            firstName="Bob",
            lastName="Manager",
            phoneNumber="+3333333333",
            email="bob@othercorp.com",
            company="Other Corp",
            jobTitle="Manager",
        )
    )
    users.append(user3)

    return users


@pytest.fixture
def email_request_data():
    """Sample email request data"""
    return {
        "subject": "Tech Meetup Invitation",
        "body": "Join us for our upcoming tech meetup!",
        "utmCampaign": "meetup-2024",
        "company": "Tech Corp",
    }


def test_send_email_api_queues_correctly(client, sample_users, email_request_data):
    """Test that email sending API queues emails correctly"""
    response = client.post("/emails/send", json=email_request_data)

    assert response.status_code == 200
    data = response.json()

    assert "message" in data
    assert data["emailsQueued"] == 2  # Should match 2 users from Tech Corp
    assert "campaignId" in data
    assert len(data["campaignId"]) > 0
    assert "queued" in data["message"].lower()


def test_send_email_api_with_job_filter(client, sample_users):
    """Test email sending with job title filter"""
    email_data = {
        "subject": "Developer Newsletter",
        "body": "Latest tech updates for developers",
        "utmCampaign": "dev-newsletter",
        "jobTitle": "Developer",
    }

    response = client.post("/emails/send", json=email_data)

    assert response.status_code == 200
    data = response.json()

    assert data["emailsQueued"] == 1  # Should match 1 developer
    assert "campaignId" in data


def test_send_email_api_with_multiple_filters(client, sample_users):
    """Test email sending with company filter (primary filter takes precedence)"""
    email_data = {
        "subject": "Tech Corp Update",
        "body": "Company-wide update for Tech Corp employees",
        "utmCampaign": "company-update",
        "company": "Tech Corp"
        # Note: Our current filtering logic uses company as primary filter
    }

    response = client.post("/emails/send", json=email_data)

    assert response.status_code == 200
    data = response.json()

    assert data["emailsQueued"] == 2  # Should match 2 users at Tech Corp
    assert "campaignId" in data


def test_send_email_api_no_matching_users(client, sample_users):
    """Test email sending when no users match the criteria"""
    email_data = {
        "subject": "No Match Email",
        "body": "This should not match any users",
        "utmCampaign": "no-match",
        "company": "Nonexistent Corp",
    }

    response = client.post("/emails/send", json=email_data)

    assert response.status_code == 200
    data = response.json()

    assert data["emailsQueued"] == 0  # No matching users
    assert "campaignId" in data


def test_send_email_api_invalid_data(client):
    """Test email sending with invalid data"""
    invalid_data = {
        "body": "Missing subject field",
        "utmCampaign": "invalid-test"
        # Missing required 'subject' field
    }

    response = client.post("/emails/send", json=invalid_data)
    assert response.status_code == 422  # Validation error


def test_send_email_api_with_event_count_filters(client, sample_users):
    """Test email sending with event count filters"""
    email_data = {
        "subject": "New User Welcome",
        "body": "Welcome to our platform!",
        "utmCampaign": "new-user-welcome",
        "hostedEventCountMin": 0,
        "hostedEventCountMax": 0,
        "attendedEventCountMin": 0,
        "attendedEventCountMax": 0,
    }

    response = client.post("/emails/send", json=email_data)

    assert response.status_code == 200
    data = response.json()

    # Should match users with 0 hosted and 0 attended events
    assert data["emailsQueued"] >= 3  # At least our 3 sample users
    assert "campaignId" in data
