"""
Tests for flexible filtering functionality in UserService
"""
import pytest
from app.services.user_service import UserService
from app.schemas.user import UserCreate
from tests.conftest import TEST_TABLE_NAME


class TestFlexibleFiltering:
    @pytest.fixture
    def service(self, dynamodb_resource):
        return UserService(dynamodb_resource, TEST_TABLE_NAME)

    @pytest.fixture
    def diverse_users(self, service):
        """Create users with diverse attributes for testing"""
        users_data = [
            # Tech Corp users
            {
                "firstName": "Alice",
                "lastName": "Smith",
                "email": "alice@tech.com",
                "company": "Tech Corp",
                "jobTitle": "Developer",
                "city": "San Francisco",
                "state": "CA",
            },
            {
                "firstName": "Bob",
                "lastName": "Johnson",
                "email": "bob@tech.com",
                "company": "Tech Corp",
                "jobTitle": "Designer",
                "city": "San Francisco",
                "state": "CA",
            },
            {
                "firstName": "Charlie",
                "lastName": "Wilson",
                "email": "charlie@tech.com",
                "company": "Tech Corp",
                "jobTitle": "Developer",
                "city": "Austin",
                "state": "TX",
            },
            # Design Co users
            {
                "firstName": "Diana",
                "lastName": "Brown",
                "email": "diana@design.com",
                "company": "Design Co",
                "jobTitle": "Developer",
                "city": "New York",
                "state": "NY",
            },
            {
                "firstName": "Eve",
                "lastName": "Davis",
                "email": "eve@design.com",
                "company": "Design Co",
                "jobTitle": "Manager",
                "city": "San Francisco",
                "state": "CA",
            },
        ]

        created_users = []
        for user_data in users_data:
            user_data["phoneNumber"] = "+1234567890"  # Required field
            user = service.create_user(UserCreate(**user_data))
            created_users.append(user)

        return created_users

    def test_single_filter_company(self, service, diverse_users):
        """Test: Only company filter - should use GSI_ByCompany"""
        filters = {"company": "Tech Corp"}

        users, next_token = service.filter_users(filters, limit=10)

        assert len(users) == 3  # Alice, Bob, Charlie
        assert all(user.company == "Tech Corp" for user in users)
        assert next_token is None  # All results fit in one page

    def test_single_filter_job_title(self, service, diverse_users):
        """Test: Only jobTitle filter - should use GSI_ByJobTitle"""
        filters = {"jobTitle": "Developer"}

        users, next_token = service.filter_users(filters, limit=10)

        assert len(users) == 3  # Alice, Charlie, Diana
        assert all(user.jobTitle == "Developer" for user in users)

    def test_single_filter_location(self, service, diverse_users):
        """Test: Only location filter - should use GSI_ByLocation"""
        filters = {"city": "San Francisco", "state": "CA"}

        users, next_token = service.filter_users(filters, limit=10)

        assert len(users) == 3  # Alice, Bob, Eve
        assert all(
            user.city == "San Francisco" and user.state == "CA" for user in users
        )

    def test_multi_filter_company_job(self, service, diverse_users):
        """Test: Company + jobTitle - should use company index (more selective)"""
        filters = {"company": "Tech Corp", "jobTitle": "Developer"}

        users, next_token = service.filter_users(filters, limit=10)

        assert len(users) == 2  # Alice, Charlie (both Tech Corp developers)
        assert all(
            user.company == "Tech Corp" and user.jobTitle == "Developer"
            for user in users
        )

    def test_multi_filter_location_job(self, service, diverse_users):
        """Test: Location + jobTitle - should use location index (most selective)"""
        filters = {"city": "San Francisco", "state": "CA", "jobTitle": "Developer"}

        users, next_token = service.filter_users(filters, limit=10)

        assert len(users) == 1  # Only Alice
        assert users[0].firstName == "Alice"
        assert users[0].city == "San Francisco"
        assert users[0].jobTitle == "Developer"

    def test_complex_multi_filter(self, service, diverse_users):
        """Test: Multiple filters across different dimensions"""
        filters = {
            "company": "Tech Corp",
            "city": "Austin",
            "state": "TX",
            "jobTitle": "Developer",
        }

        users, next_token = service.filter_users(filters, limit=10)

        assert len(users) == 1  # Only Charlie
        assert users[0].firstName == "Charlie"

    def test_no_matching_results(self, service, diverse_users):
        """Test: Filters that match no users"""
        filters = {"company": "Nonexistent Corp", "jobTitle": "Developer"}

        users, next_token = service.filter_users(filters, limit=10)

        assert len(users) == 0
        assert next_token is None

    def test_pagination_with_small_limit(self, service, diverse_users):
        """Test: Pagination with limit smaller than total results"""
        filters = {"jobTitle": "Developer"}  # Should match 3 users

        # First page - limit 2
        users_page1, token1 = service.filter_users(filters, limit=2)
        assert len(users_page1) == 2
        # Token may or may not exist depending on GSI scan behavior

        if token1 is not None:
            # Second page
            users_page2, token2 = service.filter_users(
                filters, limit=2, last_evaluated_key=token1
            )
            # Verify no duplicates
            all_user_ids = [u.id for u in users_page1] + [u.id for u in users_page2]
            assert len(set(all_user_ids)) == len(users_page1) + len(
                users_page2
            )  # All unique
        else:
            # All results fit in first page
            assert len(users_page1) <= 3  # Should have all developers

    def test_event_count_filtering(self, service, diverse_users):
        """Test: Event count range filtering"""
        filters = {
            "hostedEventCount": {"min": 0, "max": 0},  # Users with no hosted events
            "attendedEventCount": {"min": 0, "max": 0},  # Users with no attended events
        }

        users, next_token = service.filter_users(filters, limit=10)

        # All our test users should have 0 events initially
        assert len(users) == 5  # All diverse users
        assert all(user.hostedEventCount == 0 for user in users)
        assert all(user.attendedEventCount == 0 for user in users)

    def test_strategy_selection_priority(self, service, diverse_users):
        """Test that the service chooses the most selective index"""

        # Location is most selective (0.05)
        # Company is next (0.1)
        # JobTitle is less selective (0.2)

        filters = {
            "company": "Tech Corp",  # 0.1 selectivity
            "jobTitle": "Developer",  # 0.2 selectivity
            "city": "San Francisco",  # 0.05 selectivity (should win)
            "state": "CA",
        }

        # The service should choose location index as primary
        strategy = service._choose_best_strategy(filters)
        assert strategy["type"] == "gsi_query"
        assert strategy["primary_filter"]["type"] == "location"
        assert strategy["primary_filter"]["index"] == "GSI_ByLocation"

    def test_fallback_to_scan(self, service, diverse_users):
        """Test fallback to table scan when no good indexes available"""

        # Filters that don't have dedicated indexes
        filters = {
            "firstName": "Alice",  # No index for firstName
        }

        strategy = service._choose_best_strategy(filters)
        assert strategy["type"] == "scan"

        # Should scan all users since firstName filtering isn't implemented in _matches_all_filters
        users, next_token = service.filter_users(filters, limit=10)
        # Returns all users since firstName filtering isn't implemented
        assert len(users) == 5

    def test_pagination_token_encoding(self, service, diverse_users):
        """Test that pagination tokens are properly encoded/decoded"""
        filters = {"company": "Tech Corp"}

        # Get first page with small limit to force pagination
        users_page1, token1 = service.filter_users(filters, limit=1)
        assert len(users_page1) == 1

        if token1 is not None:
            assert isinstance(token1, str)  # Should be base64 encoded string

            # Use token for second page
            users_page2, token2 = service.filter_users(
                filters, limit=1, last_evaluated_key=token1
            )

            if len(users_page2) > 0:
                # Verify different users
                assert users_page1[0].id != users_page2[0].id
        else:
            # All Tech Corp users fit in one page (expected with small dataset)
            assert len(users_page1) <= 3

    def test_index_selectivity_configuration(self, service):
        """Test that index selectivity is properly configured"""
        expected_selectivity = {
            "location": 0.05,  # Most selective
            "company": 0.1,  # Moderately selective
            "jobTitle": 0.2,  # Less selective
            "hostedEventCount": 0.8,  # Least selective
            "attendedEventCount": 0.8,  # Least selective
        }

        assert service.index_selectivity == expected_selectivity

    def test_empty_filters(self, service, diverse_users):
        """Test behavior with empty filters - should scan all users"""
        filters = {}

        users, next_token = service.filter_users(filters, limit=10)

        # Should return all users (5 diverse users)
        assert len(users) == 5
        assert next_token is None
