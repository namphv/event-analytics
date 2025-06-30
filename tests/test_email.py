import pytest
from unittest.mock import patch
from app.services.email_service import EmailService
from app.services.user_service import UserService
from app.schemas.email import EmailSendRequest, EmailSendResponse
from app.schemas.user import UserCreate
from tests.conftest import TEST_TABLE_NAME


@pytest.fixture
def email_service(dynamodb_resource):
    """Create EmailService instance with test table"""
    return EmailService(dynamodb_resource, TEST_TABLE_NAME)


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
def email_request():
    """Sample email request"""
    return EmailSendRequest(
        subject="Tech Meetup Invitation",
        body="Join us for our upcoming tech meetup!",
        utmCampaign="meetup-2024",
        company="Tech Corp",
    )


def test_send_email_endpoint_queues_task(email_service, sample_users, email_request):
    """Test that email sending queues background tasks"""
    from fastapi import BackgroundTasks

    background_tasks = BackgroundTasks()
    result = email_service.send_bulk_email(email_request, background_tasks)

    assert isinstance(result, EmailSendResponse)
    assert result.emailsQueued == 2  # Should match 2 users from Tech Corp
    assert "queued" in result.message.lower()
    assert result.campaignId is not None


def test_email_worker_sends_email(email_service):
    """Test that email worker processes email successfully"""
    email_id = "test-email-123"
    user_email = "test@example.com"
    subject = "Test Subject"
    body = "Test Body"
    utm_params = {"utmCampaign": "test-campaign"}

    with patch("app.services.email_service.send_email_via_service") as mock_send:
        mock_send.return_value = True  # Simulate successful send

        email_service.process_email_task(
            email_id, user_email, subject, body, utm_params
        )

        # Should have called the email sending service
        mock_send.assert_called_once_with(user_email, subject, body)

        # Should have updated status to 'sent' in database
        email_analytics = email_service._get_email_analytics(email_id)
        assert email_analytics["status"] == "sent"
        assert email_analytics["sentAt"] is not None


def test_email_worker_handles_failure(email_service):
    """Test that email worker handles failures gracefully"""
    email_id = "test-email-456"
    user_email = "test@example.com"
    subject = "Test Subject"
    body = "Test Body"
    utm_params = {"utmCampaign": "test-campaign"}

    with patch("app.services.email_service.send_email_via_service") as mock_send:
        mock_send.side_effect = Exception("SMTP server error")

        email_service.process_email_task(
            email_id, user_email, subject, body, utm_params
        )

        # Should have updated status to 'failed' in database
        email_analytics = email_service._get_email_analytics(email_id)
        assert email_analytics["status"] == "failed"
        assert "SMTP server error" in email_analytics["errorMessage"]


def test_filter_users_for_email(email_service, sample_users):
    """Test that email service correctly filters users"""
    filters = {"company": "Tech Corp"}

    users = email_service._filter_users_for_email(filters)

    assert len(users) == 2
    assert all(user.company == "Tech Corp" for user in users)
    assert all(user.email.endswith("@techcorp.com") for user in users)


def test_create_email_analytics_items(email_service, sample_users):
    """Test creation of email analytics tracking items"""
    campaign_id = "campaign-123"
    subject = "Test Email"
    utm_params = {"utmCampaign": "test"}

    users = [sample_users[0], sample_users[1]]  # First 2 users

    email_ids = email_service._create_email_analytics_items(
        users, campaign_id, subject, utm_params
    )

    assert len(email_ids) == 2

    # Check that analytics items were created in database
    for email_id in email_ids:
        analytics = email_service._get_email_analytics(email_id)
        assert analytics["status"] == "queued"
        assert analytics["subject"] == subject
        assert analytics["utmCampaign"] == "test"
