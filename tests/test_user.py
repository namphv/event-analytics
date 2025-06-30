import pytest
from app.services.user_service import UserService
from app.schemas.user import UserCreate, UserOut
from tests.conftest import TEST_TABLE_NAME


@pytest.fixture
def user_service(dynamodb_resource):
    """Create UserService instance with test table"""
    return UserService(dynamodb_resource, TEST_TABLE_NAME)


@pytest.fixture
def valid_user_data():
    """Valid user data for testing"""
    return UserCreate(
        firstName="John",
        lastName="Doe",
        phoneNumber="+1234567890",
        email="john.doe@example.com",
        avatar="https://example.com/avatar.jpg",
        gender="Male",
        jobTitle="Software Engineer",
        company="Tech Corp",
        city="San Francisco",
        state="CA",
    )


def test_create_user_success(user_service, valid_user_data):
    """Test successful user creation"""
    result = user_service.create_user(valid_user_data)

    assert isinstance(result, UserOut)
    assert result.firstName == valid_user_data.firstName
    assert result.lastName == valid_user_data.lastName
    assert result.email == valid_user_data.email
    assert result.hostedEventCount == 0
    assert result.attendedEventCount == 0
    assert result.id is not None
    assert len(result.id) > 0


def test_create_user_invalid_data(user_service):
    """Test user creation with invalid email"""
    with pytest.raises(ValueError):
        UserCreate(
            firstName="John",
            lastName="Doe",
            phoneNumber="+1234567890",
            email="invalid-email",  # Invalid email format
            company="Tech Corp",
        )


def test_filter_by_all_criteria(user_service):
    """Test filtering users by multiple criteria"""
    # Create test users
    users = [
        UserCreate(
            firstName="Alice",
            lastName="Smith",
            phoneNumber="+1111111111",
            email="alice@techcorp.com",
            company="Tech Corp",
            jobTitle="Developer",
            city="San Francisco",
            state="CA",
        ),
        UserCreate(
            firstName="Bob",
            lastName="Johnson",
            phoneNumber="+2222222222",
            email="bob@othercorp.com",
            company="Other Corp",
            jobTitle="Designer",
            city="New York",
            state="NY",
        ),
        UserCreate(
            firstName="Charlie",
            lastName="Brown",
            phoneNumber="+3333333333",
            email="charlie@techcorp.com",
            company="Tech Corp",
            jobTitle="Manager",
            city="San Francisco",
            state="CA",
        ),
    ]

    # Create users in database
    for user in users:
        user_service.create_user(user)

    # Test filtering by company
    filters = {"company": "Tech Corp"}
    users, next_token = user_service.filter_users(filters)
    assert len(users) == 2
    assert all(user.company == "Tech Corp" for user in users)
    assert next_token is None  # No pagination needed

    # Test filtering by job title
    filters = {"jobTitle": "Developer"}
    users, next_token = user_service.filter_users(filters)
    assert len(users) == 1
    assert users[0].jobTitle == "Developer"

    # Test filtering by location
    filters = {"city": "San Francisco", "state": "CA"}
    users, next_token = user_service.filter_users(filters)
    assert len(users) == 2
    assert all(user.city == "San Francisco" and user.state == "CA" for user in users)

    # Test multiple filters
    filters = {"company": "Tech Corp", "city": "San Francisco", "state": "CA"}
    users, next_token = user_service.filter_users(filters)
    assert len(users) == 2
    assert all(
        user.company == "Tech Corp" and user.city == "San Francisco" for user in users
    )
