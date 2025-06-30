import uuid
from boto3.dynamodb.conditions import Key, Attr
from app.schemas.user import UserCreate, UserOut
from botocore.exceptions import ClientError


class UserService:
    def __init__(self, dynamodb_resource, table_name="CommunityApp"):
        self.dynamodb = dynamodb_resource
        self.table = dynamodb_resource.Table(table_name)

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

    def filter_users(self, filters: dict) -> list[UserOut]:
        """Filter users based on multiple criteria"""
        items = []

        # Determine which GSI to use based on filters
        if "company" in filters:
            items = self._query_by_company(filters)
        elif "jobTitle" in filters:
            items = self._query_by_job_title(filters)
        elif "city" in filters and "state" in filters:
            items = self._query_by_location(filters)
        elif "hostedEventCount" in filters:
            items = self._query_by_hosted_count(filters)
        elif "attendedEventCount" in filters:
            items = self._query_by_attended_count(filters)
        else:
            # Scan all user profiles if no specific filter
            items = self._scan_user_profiles(filters)

        # Convert to UserOut objects
        users = []
        for item in items:
            user_data = {
                k: v
                for k, v in item.items()
                if k not in ["PK", "SK"] and not k.startswith("GSI_")
            }
            users.append(UserOut(**user_data))

        return users

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
            new_count = response["Attributes"]["hostedEventCount"]
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
            new_count = response["Attributes"]["attendedEventCount"]
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
