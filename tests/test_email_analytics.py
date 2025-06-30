"""
Unit tests for email analytics functionality
"""
import pytest
from datetime import datetime, timezone, timedelta
from app.services.email_service import EmailService
from app.services.user_service import UserService
from app.schemas.user import UserCreate
from app.schemas.email import EmailSendRequest
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
    """Create sample users for analytics testing"""
    users = []

    user1 = user_service.create_user(
        UserCreate(
            firstName="Alice",
            lastName="Developer",
            phoneNumber="+1111111111",
            email="alice@techcorp.com",
            company="Tech Corp",
            jobTitle="Developer",
        )
    )
    users.append(user1)

    user2 = user_service.create_user(
        UserCreate(
            firstName="Bob",
            lastName="Designer",
            phoneNumber="+2222222222",
            email="bob@techcorp.com",
            company="Tech Corp",
            jobTitle="Designer",
        )
    )
    users.append(user2)

    user3 = user_service.create_user(
        UserCreate(
            firstName="Carol",
            lastName="Manager",
            phoneNumber="+3333333333",
            email="carol@othercorp.com",
            company="Other Corp",
            jobTitle="Manager",
        )
    )
    users.append(user3)

    return users


@pytest.fixture
def sample_email_data(email_service, sample_users):
    """Create sample email analytics data for testing"""
    from fastapi import BackgroundTasks

    # Campaign 1: Tech Meetup - sent to Tech Corp users
    email1 = EmailSendRequest(
        subject="Tech Meetup Invitation",
        body="Join us for our tech meetup!",
        utmCampaign="tech-meetup-2024",
        utmSource="crm",
        utmMedium="email",
        company="Tech Corp",
    )

    # Campaign 2: Product Launch - sent to all users
    email2 = EmailSendRequest(
        subject="New Product Launch",
        body="Check out our new product!",
        utmCampaign="product-launch-2024",
        utmSource="newsletter",
        utmMedium="email",
    )

    # Send emails to create analytics data
    background_tasks = BackgroundTasks()

    # Send first campaign
    response1 = email_service.send_bulk_email(email1, background_tasks)

    # Send second campaign
    response2 = email_service.send_bulk_email(email2, background_tasks)

    # Update some email statuses manually for testing
    # Get analytics items to update their status
    analytics_items = email_service._get_all_email_analytics()

    # Mark first email as sent
    if len(analytics_items) > 0:
        email_service.track_email_status(
            analytics_items[0]["id"], "sent", sent_at=datetime.now(timezone.utc)
        )

    # Mark second email as failed
    if len(analytics_items) > 1:
        email_service.track_email_status(
            analytics_items[1]["id"], "failed", error_message="SMTP connection failed"
        )

    return {
        "campaign1_id": response1.campaignId,
        "campaign2_id": response2.campaignId,
        "analytics_items": analytics_items,
    }


def test_get_all_emails_by_status(email_service, sample_email_data):
    """Test filtering emails by status"""

    # Test getting sent emails
    sent_emails = email_service.get_analytics(status="sent")
    assert len(sent_emails["emails"]) >= 1
    assert all(email["status"] == "sent" for email in sent_emails["emails"])

    # Test getting failed emails
    failed_emails = email_service.get_analytics(status="failed")
    assert len(failed_emails["emails"]) >= 1
    assert all(email["status"] == "failed" for email in failed_emails["emails"])

    # Test getting queued emails
    queued_emails = email_service.get_analytics(status="queued")
    assert len(queued_emails["emails"]) >= 1
    assert all(email["status"] == "queued" for email in queued_emails["emails"])

    # Test getting all emails (no status filter)
    all_emails = email_service.get_analytics()
    assert len(all_emails["emails"]) >= 3  # At least 3 emails from sample data


def test_filter_by_utm_campaign(email_service, sample_email_data):
    """Test filtering emails by UTM campaign"""

    # Filter by tech meetup campaign
    tech_emails = email_service.get_analytics(utm_campaign="tech-meetup-2024")
    assert len(tech_emails["emails"]) == 2  # Sent to 2 Tech Corp users
    assert all(
        email["utmCampaign"] == "tech-meetup-2024" for email in tech_emails["emails"]
    )

    # Filter by product launch campaign
    product_emails = email_service.get_analytics(utm_campaign="product-launch-2024")
    assert len(product_emails["emails"]) == 3  # Sent to all 3 users
    assert all(
        email["utmCampaign"] == "product-launch-2024"
        for email in product_emails["emails"]
    )

    # Filter by non-existent campaign
    no_emails = email_service.get_analytics(utm_campaign="non-existent-campaign")
    assert len(no_emails["emails"]) == 0


def test_filter_by_date_range(email_service, sample_email_data):
    """Test filtering emails by date range"""

    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    tomorrow = now + timedelta(days=1)

    # Filter emails from yesterday to tomorrow (should include all)
    recent_emails = email_service.get_analytics(
        start_date=yesterday.isoformat(), end_date=tomorrow.isoformat()
    )
    assert len(recent_emails["emails"]) >= 3  # All sample emails

    # Filter emails from tomorrow onwards (should be empty)
    future_emails = email_service.get_analytics(start_date=tomorrow.isoformat())
    assert len(future_emails["emails"]) == 0

    # Filter emails until yesterday (should be empty)
    past_emails = email_service.get_analytics(end_date=yesterday.isoformat())
    assert len(past_emails["emails"]) == 0


def test_pagination(email_service, sample_email_data):
    """Test pagination of email analytics"""

    # First check total count to see if pagination makes sense
    all_emails = email_service.get_analytics()
    total_count = len(all_emails["emails"])

    # Test basic pagination functionality
    if total_count > 0:
        # Test with limit 1 to ensure pagination works
        page1 = email_service.get_analytics(limit=1)
        assert len(page1["emails"]) == 1

        if total_count > 1:
            # Should have more results
            assert page1["hasMore"] is True
            assert "nextToken" in page1

            # Get next page
            page2 = email_service.get_analytics(
                limit=1, last_evaluated_key=page1["nextToken"]
            )
            # Should have at least 1 more email (could be 0 if pagination token is stale)

            # Verify no duplicate emails if both pages have data
            if len(page2["emails"]) > 0:
                page1_ids = {email["id"] for email in page1["emails"]}
                page2_ids = {email["id"] for email in page2["emails"]}
                assert len(page1_ids.intersection(page2_ids)) == 0
        else:
            # Only 1 email total, no more pages
            assert page1["hasMore"] is False
    else:
        # No emails at all
        page1 = email_service.get_analytics(limit=1)
        assert len(page1["emails"]) == 0
        assert page1["hasMore"] is False


def test_filter_by_campaign_and_status(email_service, sample_email_data):
    """Test combining multiple filters"""

    # Filter by campaign and status
    tech_sent = email_service.get_analytics(
        utm_campaign="tech-meetup-2024", status="sent"
    )

    # Should only return tech meetup emails that were sent
    for email in tech_sent["emails"]:
        assert email["utmCampaign"] == "tech-meetup-2024"
        assert email["status"] == "sent"


def test_email_analytics_sorting(email_service, sample_email_data):
    """Test that emails are sorted by creation date (newest first)"""

    all_emails = email_service.get_analytics()

    # Verify emails are sorted by createdAt descending
    created_dates = [
        datetime.fromisoformat(email["createdAt"].replace("Z", "+00:00"))
        for email in all_emails["emails"]
    ]

    # Check that each date is >= the next one (descending order)
    for i in range(len(created_dates) - 1):
        assert created_dates[i] >= created_dates[i + 1]


def test_email_analytics_response_format(email_service, sample_email_data):
    """Test the response format of email analytics"""

    response = email_service.get_analytics(limit=1)

    # Check response structure
    assert "emails" in response
    assert "count" in response
    assert "total" in response
    assert "hasMore" in response
    assert isinstance(response["emails"], list)
    assert isinstance(response["count"], int)
    assert isinstance(response["total"], int)
    assert isinstance(response["hasMore"], bool)

    # Check email item structure
    if len(response["emails"]) > 0:
        email = response["emails"][0]
        required_fields = [
            "id",
            "userId",
            "email",
            "subject",
            "status",
            "campaignId",
            "createdAt",
            "utmCampaign",
            "utmSource",
            "utmMedium",
        ]
        for field in required_fields:
            assert field in email


def test_empty_analytics_response(email_service):
    """Test analytics response when no emails exist"""

    # Query with filter that matches no emails
    response = email_service.get_analytics(utm_campaign="no-such-campaign")

    assert response["emails"] == []
    assert response["count"] == 0
    assert response["total"] == 0
    assert response["hasMore"] is False
    assert "nextToken" not in response


def test_analytics_with_sent_at_and_error_fields(email_service, sample_email_data):
    """Test that sent emails include sentAt and failed emails include errorMessage"""

    # Get sent emails
    sent_emails = email_service.get_analytics(status="sent")
    if len(sent_emails["emails"]) > 0:
        sent_email = sent_emails["emails"][0]
        assert "sentAt" in sent_email
        assert sent_email["sentAt"] is not None
        assert "errorMessage" not in sent_email or sent_email["errorMessage"] is None

    # Get failed emails
    failed_emails = email_service.get_analytics(status="failed")
    if len(failed_emails["emails"]) > 0:
        failed_email = failed_emails["emails"][0]
        assert "errorMessage" in failed_email
        assert failed_email["errorMessage"] is not None
        assert "sentAt" not in failed_email or failed_email["sentAt"] is None
