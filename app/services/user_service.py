import uuid
import json
import base64
from typing import Dict, Any, List, Optional, Tuple
from boto3.dynamodb.conditions import Key, Attr
from app.schemas.user import UserCreate, UserOut
from botocore.exceptions import ClientError


class UserService:
    def __init__(self, dynamodb_resource, table_name="CommunityApp"):
        self.dynamodb = dynamodb_resource
        self.table = dynamodb_resource.Table(table_name)

        # Index selectivity estimates (lower = more selective)
        self.index_selectivity = {
            "location": 0.05,  # 5% of users per city/state combination
            "company": 0.1,  # 10% of users per company
            "jobTitle": 0.2,  # 20% of users per job title
            "hostedEventCount": 0.8,  # 80% of users in typical count ranges
            "attendedEventCount": 0.8,  # 80% of users in typical count ranges
        }

    def create_user(self, user_data: UserCreate) -> UserOut:
        """Create a new user with initialized counters"""
        user_id = str(uuid.uuid4())

        # Prepare item for DynamoDB
        item = {
            "PK": f"USER#{user_id}",
            "SK": "PROFILE",
            "id": user_id,
            "firstName": user_data.firstName,
            "lastName": user_data.lastName,
            "phoneNumber": user_data.phoneNumber,
            "email": user_data.email,
            "hostedEventCount": 0,
            "attendedEventCount": 0,
        }

        # Add optional fields
        if user_data.avatar:
            item["avatar"] = user_data.avatar
        if user_data.gender:
            item["gender"] = user_data.gender
        if user_data.jobTitle:
            item["jobTitle"] = user_data.jobTitle
        if user_data.company:
            item["company"] = user_data.company
        if user_data.city:
            item["city"] = user_data.city
        if user_data.state:
            item["state"] = user_data.state

        # Add GSI attributes for filtering
        if user_data.company:
            item["GSI_ByCompany_PK"] = f"COMPANY#{user_data.company}"
            item["GSI_ByCompany_SK"] = f"LASTNAME#{user_data.lastName}#USER#{user_id}"

        if user_data.jobTitle:
            item["GSI_ByJobTitle_PK"] = f"JOBTITLE#{user_data.jobTitle}"
            item["GSI_ByJobTitle_SK"] = f"LASTNAME#{user_data.lastName}#USER#{user_id}"

        if user_data.city and user_data.state:
            item["GSI_ByLocation_PK"] = f"LOCATION#{user_data.state}#{user_data.city}"
            item["GSI_ByLocation_SK"] = f"LASTNAME#{user_data.lastName}#USER#{user_id}"

        # Add user activity GSI attributes
        item["GSI_UsersByHostedCount_PK"] = "USER_PROFILE"
        item["GSI_UsersByHostedCount_SK"] = f"HOSTED_COUNT#{0:010d}#USER#{user_id}"

        item["GSI_UsersByAttendedCount_PK"] = "USER_PROFILE"
        item["GSI_UsersByAttendedCount_SK"] = f"ATTENDED_COUNT#{0:010d}#USER#{user_id}"

        item["GSI_UsersByActivity_PK"] = "USER_ACTIVITY"
        item["GSI_UsersByActivity_SK"] = f"ATTENDED_COUNT#{0:010d}#USER#{user_id}"

        try:
            self.table.put_item(Item=item)
            return UserOut(
                **{
                    k: v
                    for k, v in item.items()
                    if k
                    not in [
                        "PK",
                        "SK",
                        "GSI_ByCompany_PK",
                        "GSI_ByCompany_SK",
                        "GSI_ByJobTitle_PK",
                        "GSI_ByJobTitle_SK",
                        "GSI_ByLocation_PK",
                        "GSI_ByLocation_SK",
                        "GSI_UsersByHostedCount_PK",
                        "GSI_UsersByHostedCount_SK",
                        "GSI_UsersByAttendedCount_PK",
                        "GSI_UsersByAttendedCount_SK",
                        "GSI_UsersByActivity_PK",
                        "GSI_UsersByActivity_SK",
                    ]
                }
            )
        except ClientError as e:
            raise Exception(f"Failed to create user: {e}")

    def filter_users(
        self,
        filters: Dict[str, Any],
        limit: int = 50,
        last_evaluated_key: Optional[str] = None,
    ) -> Tuple[List[UserOut], Optional[str]]:
        """
        Flexible filtering that chooses the best strategy based on available filters
        Returns: (users, next_last_evaluated_key)
        """

        # Step 1: Choose the best index strategy
        strategy = self._choose_best_strategy(filters)

        # Step 2: Execute the strategy with pagination
        if strategy["type"] == "gsi_query":
            return self._query_with_pagination(
                strategy, filters, limit, last_evaluated_key
            )
        elif strategy["type"] == "scan":
            return self._scan_with_pagination(filters, limit, last_evaluated_key)
        else:
            raise ValueError(f"Unknown strategy type: {strategy['type']}")

    def _choose_best_strategy(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Intelligently choose the best query strategy based on available filters
        """

        available_filters = []

        # Analyze available filters and their selectivity
        if "company" in filters:
            available_filters.append(
                {
                    "type": "company",
                    "selectivity": self.index_selectivity["company"],
                    "index": "GSI_ByCompany",
                    "pk": f"COMPANY#{filters['company']}",
                    "pk_attr": "GSI_ByCompany_PK",
                }
            )

        if "jobTitle" in filters:
            available_filters.append(
                {
                    "type": "jobTitle",
                    "selectivity": self.index_selectivity["jobTitle"],
                    "index": "GSI_ByJobTitle",
                    "pk": f"JOBTITLE#{filters['jobTitle']}",
                    "pk_attr": "GSI_ByJobTitle_PK",
                }
            )

        if "city" in filters and "state" in filters:
            available_filters.append(
                {
                    "type": "location",
                    "selectivity": self.index_selectivity["location"],
                    "index": "GSI_ByLocation",
                    "pk": f"LOCATION#{filters['state']}#{filters['city']}",
                    "pk_attr": "GSI_ByLocation_PK",
                }
            )

        if "hostedEventCount" in filters:
            available_filters.append(
                {
                    "type": "hostedEventCount",
                    "selectivity": self.index_selectivity["hostedEventCount"],
                    "index": "GSI_UsersByHostedCount",
                    "pk": "USER_PROFILE",
                    "pk_attr": "GSI_UsersByHostedCount_PK",
                }
            )

        if "attendedEventCount" in filters:
            available_filters.append(
                {
                    "type": "attendedEventCount",
                    "selectivity": self.index_selectivity["attendedEventCount"],
                    "index": "GSI_UsersByAttendedCount",
                    "pk": "USER_PROFILE",
                    "pk_attr": "GSI_UsersByAttendedCount_PK",
                }
            )

        # Choose the most selective index (lowest selectivity value)
        if available_filters:
            best_filter = min(available_filters, key=lambda x: x["selectivity"])
            return {
                "type": "gsi_query",
                "primary_filter": best_filter,
                "secondary_filters": [f for f in available_filters if f != best_filter],
            }
        else:
            # No good indexes available - use scan
            return {"type": "scan"}

    def _query_with_pagination(
        self,
        strategy: Dict[str, Any],
        filters: Dict[str, Any],
        limit: int,
        last_evaluated_key: Optional[str],
    ) -> Tuple[List[UserOut], Optional[str]]:
        """
        Query using the best GSI with pagination support
        """

        primary_filter = strategy["primary_filter"]
        users = []

        # Parse last_evaluated_key
        exclusive_start_key = None
        if last_evaluated_key:
            try:
                exclusive_start_key = json.loads(
                    base64.b64decode(last_evaluated_key).decode()
                )
            except (ValueError, json.JSONDecodeError):
                exclusive_start_key = None

        # Keep querying until we have enough results or run out of data
        while len(users) < limit:
            # Calculate how many more we need (get extra to account for filtering)
            needed = limit - len(users)
            query_limit = min(needed * 3, 100)  # Get 3x what we need, max 100 per query

            # Build query parameters
            query_params = {
                "IndexName": primary_filter["index"],
                "KeyConditionExpression": Key(primary_filter["pk_attr"]).eq(
                    primary_filter["pk"]
                ),
                "Limit": query_limit,
            }

            if exclusive_start_key:
                query_params["ExclusiveStartKey"] = exclusive_start_key

            # Execute query
            response = self.table.query(**query_params)

            # Filter results in memory
            for item in response.get("Items", []):
                if self._matches_all_filters(item, filters):
                    user_data = self._clean_dynamodb_fields(item)
                    users.append(UserOut(**user_data))

                    if len(users) >= limit:
                        break

            # Check if we have more data
            exclusive_start_key = response.get("LastEvaluatedKey")
            if not exclusive_start_key:
                break  # No more data available

        # Encode next pagination token
        next_token = None
        if exclusive_start_key and len(users) == limit:
            next_token = base64.b64encode(
                json.dumps(exclusive_start_key).encode()
            ).decode()

        return users, next_token

    def _scan_with_pagination(
        self, filters: Dict[str, Any], limit: int, last_evaluated_key: Optional[str]
    ) -> Tuple[List[UserOut], Optional[str]]:
        """
        Fallback scan when no good indexes available
        """

        users = []

        # Parse last_evaluated_key
        exclusive_start_key = None
        if last_evaluated_key:
            try:
                exclusive_start_key = json.loads(
                    base64.b64decode(last_evaluated_key).decode()
                )
            except (ValueError, json.JSONDecodeError):
                exclusive_start_key = None

        # Keep scanning until we have enough results
        while len(users) < limit:
            needed = limit - len(users)
            scan_limit = min(needed * 5, 100)  # Get 5x what we need for scan

            # Build scan parameters
            scan_params = {
                "FilterExpression": Attr("SK").eq("PROFILE"),
                "Limit": scan_limit,
            }

            if exclusive_start_key:
                scan_params["ExclusiveStartKey"] = exclusive_start_key

            # Execute scan
            response = self.table.scan(**scan_params)

            # Filter results
            for item in response.get("Items", []):
                if self._matches_all_filters(item, filters):
                    user_data = self._clean_dynamodb_fields(item)
                    users.append(UserOut(**user_data))

                    if len(users) >= limit:
                        break

            # Check for more data
            exclusive_start_key = response.get("LastEvaluatedKey")
            if not exclusive_start_key:
                break

        # Encode next token
        next_token = None
        if exclusive_start_key and len(users) == limit:
            next_token = base64.b64encode(
                json.dumps(exclusive_start_key).encode()
            ).decode()

        return users, next_token

    def _matches_all_filters(self, item: Dict, filters: Dict[str, Any]) -> bool:
        """Check if item matches ALL filter criteria"""

        # Company filter
        if "company" in filters:
            if item.get("company") != filters["company"]:
                return False

        # Job title filter
        if "jobTitle" in filters:
            if item.get("jobTitle") != filters["jobTitle"]:
                return False

        # Location filter
        if "city" in filters and "state" in filters:
            if (
                item.get("city") != filters["city"]
                or item.get("state") != filters["state"]
            ):
                return False

        # Hosted event count range
        if "hostedEventCount" in filters:
            count = item.get("hostedEventCount", 0)
            count_filter = filters["hostedEventCount"]

            if "min" in count_filter and count < count_filter["min"]:
                return False
            if "max" in count_filter and count > count_filter["max"]:
                return False

        # Attended event count range
        if "attendedEventCount" in filters:
            count = item.get("attendedEventCount", 0)
            count_filter = filters["attendedEventCount"]

            if "min" in count_filter and count < count_filter["min"]:
                return False
            if "max" in count_filter and count > count_filter["max"]:
                return False

        return True

    def _clean_dynamodb_fields(self, item: Dict) -> Dict:
        """Remove DynamoDB internal fields"""
        return {
            k: v
            for k, v in item.items()
            if k not in ["PK", "SK"] and not k.startswith("GSI_")
        }

    def _query_by_company(self, filters: dict) -> list:
        """Query users by company using GSI_ByCompany"""
        company = filters["company"]
        response = self.table.query(
            IndexName="GSI_ByCompany",
            KeyConditionExpression=Key("GSI_ByCompany_PK").eq(f"COMPANY#{company}"),
        )
        return response["Items"]

    def _query_by_job_title(self, filters: dict) -> list:
        """Query users by job title using GSI_ByJobTitle"""
        job_title = filters["jobTitle"]
        response = self.table.query(
            IndexName="GSI_ByJobTitle",
            KeyConditionExpression=Key("GSI_ByJobTitle_PK").eq(f"JOBTITLE#{job_title}"),
        )
        return response["Items"]

    def _query_by_location(self, filters: dict) -> list:
        """Query users by location using GSI_ByLocation"""
        city = filters["city"]
        state = filters["state"]
        response = self.table.query(
            IndexName="GSI_ByLocation",
            KeyConditionExpression=Key("GSI_ByLocation_PK").eq(
                f"LOCATION#{state}#{city}"
            ),
        )
        return response["Items"]

    def _query_by_hosted_count(self, filters: dict) -> list:
        """Query users by hosted event count range"""
        response = self.table.query(
            IndexName="GSI_UsersByHostedCount",
            KeyConditionExpression=Key("GSI_UsersByHostedCount_PK").eq("USER_PROFILE"),
        )

        # Apply range filter
        items = response["Items"]
        if "hostedEventCount" in filters:
            count_filter = filters["hostedEventCount"]
            if "min" in count_filter:
                items = [
                    item
                    for item in items
                    if item.get("hostedEventCount", 0) >= count_filter["min"]
                ]
            if "max" in count_filter:
                items = [
                    item
                    for item in items
                    if item.get("hostedEventCount", 0) <= count_filter["max"]
                ]

        return items

    def _query_by_attended_count(self, filters: dict) -> list:
        """Query users by attended event count range"""
        response = self.table.query(
            IndexName="GSI_UsersByAttendedCount",
            KeyConditionExpression=Key("GSI_UsersByAttendedCount_PK").eq(
                "USER_PROFILE"
            ),
        )

        # Apply range filter
        items = response["Items"]
        if "attendedEventCount" in filters:
            count_filter = filters["attendedEventCount"]
            if "min" in count_filter:
                items = [
                    item
                    for item in items
                    if item.get("attendedEventCount", 0) >= count_filter["min"]
                ]
            if "max" in count_filter:
                items = [
                    item
                    for item in items
                    if item.get("attendedEventCount", 0) <= count_filter["max"]
                ]

        return items

    def _scan_user_profiles(self, filters: dict) -> list:
        """Scan for user profiles when no GSI can be used"""
        scan_kwargs = {"FilterExpression": Attr("SK").eq("PROFILE")}

        response = self.table.scan(**scan_kwargs)
        return response["Items"]

    def increment_hosted_count(self, user_id: str):
        """Increment the hosted event count for a user"""
        try:
            response = self.table.update_item(
                Key={"PK": f"USER#{user_id}", "SK": "PROFILE"},
                UpdateExpression="ADD hostedEventCount :inc",
                ExpressionAttributeValues={":inc": 1},
                ReturnValues="ALL_NEW",
            )

            # Update GSI sort key for hosted count
            new_count = int(response["Attributes"]["hostedEventCount"])
            self.table.update_item(
                Key={"PK": f"USER#{user_id}", "SK": "PROFILE"},
                UpdateExpression="SET GSI_UsersByHostedCount_SK = :sk",
                ExpressionAttributeValues={
                    ":sk": f"HOSTED_COUNT#{new_count:010d}#USER#{user_id}"
                },
            )

        except ClientError as e:
            raise Exception(f"Failed to increment hosted count: {e}")

    def increment_attended_count(self, user_id: str):
        """Increment the attended event count for a user"""
        try:
            response = self.table.update_item(
                Key={"PK": f"USER#{user_id}", "SK": "PROFILE"},
                UpdateExpression="ADD attendedEventCount :inc",
                ExpressionAttributeValues={":inc": 1},
                ReturnValues="ALL_NEW",
            )

            # Update GSI sort keys for attended count
            new_count = int(response["Attributes"]["attendedEventCount"])
            self.table.update_item(
                Key={"PK": f"USER#{user_id}", "SK": "PROFILE"},
                UpdateExpression="SET GSI_UsersByAttendedCount_SK = :sk1, GSI_UsersByActivity_SK = :sk2",
                ExpressionAttributeValues={
                    ":sk1": f"ATTENDED_COUNT#{new_count:010d}#USER#{user_id}",
                    ":sk2": f"ATTENDED_COUNT#{new_count:010d}#USER#{user_id}",
                },
            )

        except ClientError as e:
            raise Exception(f"Failed to increment attended count: {e}")
