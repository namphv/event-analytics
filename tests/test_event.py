import pytest
from datetime import datetime, timezone
from app.services.event_service import EventService
from app.services.user_service import UserService
from app.schemas.event import EventCreate, EventOut
from app.schemas.user import UserCreate
from tests.conftest import TEST_TABLE_NAME


@pytest.fixture
def event_service(dynamodb_resource):
    """Create EventService instance with test table"""
    return EventService(dynamodb_resource, TEST_TABLE_NAME)


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

    user3 = user_service.create_user(
        UserCreate(
            firstName="Bob",
            lastName="Participant",
            phoneNumber="+3333333333",
            email="bob@example.com",
            company="Dev Inc",
        )
    )
    users.append(user3)

    return users


@pytest.fixture
def valid_event_data():
    """Valid event data for testing"""
    return EventCreate(
        slug="tech-meetup-2024",
        title="Tech Meetup 2024",
        description="A great tech meetup",
        startAt=datetime(2024, 3, 15, 18, 0, tzinfo=timezone.utc),
        endAt=datetime(2024, 3, 15, 20, 0, tzinfo=timezone.utc),
        venue="Tech Hub",
        maxCapacity=100,
        owner="owner-user-id",
    )


def test_create_event_success(event_service, valid_event_data):
    """Test basic event creation with no participants"""
    result = event_service.create_event(valid_event_data)

    assert isinstance(result, EventOut)
    assert result.slug == valid_event_data.slug
    assert result.title == valid_event_data.title
    assert result.description == valid_event_data.description
    assert result.venue == valid_event_data.venue
    assert result.maxCapacity == valid_event_data.maxCapacity
    assert result.owner == valid_event_data.owner
    assert result.attendeeCount == 0
    assert result.id is not None
    assert len(result.id) > 0


def test_create_event_with_participants(event_service, user_service, sample_users):
    """Test event creation with hosts and attendees"""
    host_ids = [sample_users[0].id]
    attendee_ids = [sample_users[1].id, sample_users[2].id]

    event_data = EventCreate(
        slug="team-event-2024",
        title="Team Event 2024",
        description="Team building event",
        startAt=datetime(2024, 4, 20, 10, 0, tzinfo=timezone.utc),
        endAt=datetime(2024, 4, 20, 16, 0, tzinfo=timezone.utc),
        venue="Conference Center",
        maxCapacity=50,
        owner=sample_users[0].id,
        hostIds=host_ids,
        attendeeIds=attendee_ids,
    )

    result = event_service.create_event(event_data)

    # Check event details
    assert isinstance(result, EventOut)
    assert result.title == event_data.title
    assert result.attendeeCount == 2  # Two attendees

    # Check that user counts were incremented
    # Host should have hosted count incremented
    host_user = user_service.filter_users({"company": "Tech Corp"})[0]
    assert host_user.hostedEventCount == 1

    # Attendees should have attended count incremented
    attendee1 = user_service.filter_users({"company": "Design Co"})[0]
    attendee2 = user_service.filter_users({"company": "Dev Inc"})[0]
    assert attendee1.attendedEventCount == 1
    assert attendee2.attendedEventCount == 1


def test_create_event_transaction_fails_if_user_not_found(event_service):
    """Test that transaction fails cleanly if user doesn't exist"""
    event_data = EventCreate(
        slug="failing-event",
        title="Failing Event",
        description="This should fail",
        startAt=datetime(2024, 5, 1, 9, 0, tzinfo=timezone.utc),
        endAt=datetime(2024, 5, 1, 17, 0, tzinfo=timezone.utc),
        venue="Nowhere",
        maxCapacity=10,
        owner="nonexistent-user-id",
        hostIds=["nonexistent-host-id"],
        attendeeIds=["nonexistent-attendee-id"],
    )

    with pytest.raises(Exception) as exc_info:
        event_service.create_event(event_data)

    # Should fail due to nonexistent users
    assert "Transaction failed" in str(exc_info.value) or "not found" in str(
        exc_info.value
    )
