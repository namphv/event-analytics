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


def test_get_email_analytics_api_response(client, sample_users):
    """Test email analytics API endpoint"""

    # First send some emails to create analytics data
    email_data1 = {
        "subject": "Tech Meetup Invitation",
        "body": "Join us for our tech meetup!",
        "utmCampaign": "tech-meetup-2024",
        "company": "Tech Corp",
    }

    email_data2 = {
        "subject": "Product Launch",
        "body": "Check out our new product!",
        "utmCampaign": "product-launch-2024",
    }

    # Send emails
    client.post("/emails/send", json=email_data1)
    client.post("/emails/send", json=email_data2)

    # Test getting all analytics
    response = client.get("/emails/analytics")
    assert response.status_code == 200

    data = response.json()
    assert "emails" in data
    assert "count" in data
    assert "total" in data
    assert "hasMore" in data
    assert isinstance(data["emails"], list)
    assert len(data["emails"]) >= 3  # At least 3 emails should be created

    # Test filtering by status
    response = client.get("/emails/analytics?status=queued")
    assert response.status_code == 200
    data = response.json()
    assert all(email["status"] == "queued" for email in data["emails"])

    # Test filtering by campaign
    response = client.get("/emails/analytics?utm_campaign=tech-meetup-2024")
    assert response.status_code == 200
    data = response.json()
    assert len(data["emails"]) == 2  # Should match 2 Tech Corp users
    assert all(email["utmCampaign"] == "tech-meetup-2024" for email in data["emails"])

    # Test pagination
    response = client.get("/emails/analytics?limit=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data["emails"]) <= 2
    assert data["count"] <= 2


def test_email_analytics_date_filtering(client, sample_users):
    """Test email analytics date range filtering"""
    from datetime import datetime, timezone, timedelta
    import urllib.parse

    # Send an email first
    email_data = {
        "subject": "Test Email",
        "body": "Test body",
        "utmCampaign": "test-campaign",
    }
    response = client.post("/emails/send", json=email_data)
    assert response.status_code == 200

    # Test date filtering
    now = datetime.now(timezone.utc)
    # Use wider time ranges to ensure we capture the emails
    two_days_ago = (now - timedelta(days=2)).isoformat()
    two_days_from_now = (now + timedelta(days=2)).isoformat()

    # URL encode the dates to handle + signs properly
    encoded_start = urllib.parse.quote(two_days_ago, safe="")
    encoded_end = urllib.parse.quote(two_days_from_now, safe="")

    # Get all emails first to verify they exist and check their dates
    response = client.get("/emails/analytics")
    assert response.status_code == 200
    all_data = response.json()
    total_emails = len(all_data["emails"])
    assert total_emails >= 1  # Ensure we have emails to test with

    # Get emails from 2 days ago to 2 days from now (should include our email)
    response = client.get(
        f"/emails/analytics?start_date={encoded_start}&end_date={encoded_end}"
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["emails"]) >= 1

    # Get emails from 2 days from now onwards (should be empty)
    future_date = (now + timedelta(days=3)).isoformat()
    encoded_future = urllib.parse.quote(future_date, safe="")
    response = client.get(f"/emails/analytics?start_date={encoded_future}")
    assert response.status_code == 200
    data = response.json()
    assert len(data["emails"]) == 0


def test_email_analytics_multiple_filters(client, sample_users):
    """Test combining multiple filters in email analytics"""

    # Send emails with different campaigns
    email_data1 = {
        "subject": "Newsletter",
        "body": "Newsletter content",
        "utmCampaign": "newsletter-2024",
        "utmSource": "newsletter",
        "company": "Tech Corp",
    }

    email_data2 = {
        "subject": "Promotion",
        "body": "Promotion content",
        "utmCampaign": "promotion-2024",
        "utmSource": "crm",
    }

    client.post("/emails/send", json=email_data1)
    client.post("/emails/send", json=email_data2)

    # Filter by campaign and source
    response = client.get(
        "/emails/analytics?utm_campaign=newsletter-2024&utm_source=newsletter"
    )
    assert response.status_code == 200
    data = response.json()

    # Should only return newsletter emails from newsletter source
    for email in data["emails"]:
        assert email["utmCampaign"] == "newsletter-2024"
        assert email["utmSource"] == "newsletter"


def test_email_analytics_empty_results(client):
    """Test analytics with no matching results"""

    response = client.get("/emails/analytics?utm_campaign=non-existent-campaign")
    assert response.status_code == 200
    data = response.json()

    assert data["emails"] == []
    assert data["count"] == 0
    assert data["total"] == 0
    assert data["hasMore"] is False
    assert "nextToken" not in data


def test_email_analytics_invalid_parameters(client):
    """Test analytics with invalid parameters"""

    # Test invalid limit
    response = client.get("/emails/analytics?limit=0")
    assert response.status_code == 422  # Validation error

    response = client.get("/emails/analytics?limit=101")
    assert response.status_code == 422  # Validation error

    # Test invalid date format - this might return 200 with no results if date parsing fails gracefully
    response = client.get("/emails/analytics?start_date=invalid-date")
    # Date parsing errors should either return 500 or empty results
    assert response.status_code in [200, 500]
    if response.status_code == 200:
        data = response.json()
        # If it returns 200, it should return empty results
        assert len(data["emails"]) == 0
