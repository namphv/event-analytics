import uuid
import json
import base64
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from fastapi import BackgroundTasks
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from app.schemas.email import EmailSendRequest, EmailSendResponse
from app.schemas.user import UserOut
from app.services.user_service import UserService


def send_email_via_service(email: str, subject: str, body: str) -> bool:
    """Mock email sending service - replace with actual email provider"""
    # In real implementation, this would call SendGrid, SES, etc.
    print(f"Sending email to {email}: {subject}")
    return True


class EmailService:
    def __init__(self, dynamodb_resource, table_name="CommunityApp"):
        self.dynamodb = dynamodb_resource
        self.table = dynamodb_resource.Table(table_name)
        self.user_service = UserService(dynamodb_resource, table_name)

    def send_bulk_email(
        self, email_request: EmailSendRequest, background_tasks: BackgroundTasks = None
    ) -> EmailSendResponse:
        """Send emails to filtered users using background tasks"""
        campaign_id = str(uuid.uuid4())

        # Build filter criteria from email request
        filters = {}
        if email_request.company:
            filters["company"] = email_request.company
        if email_request.jobTitle:
            filters["jobTitle"] = email_request.jobTitle
        if email_request.city and email_request.state:
            filters["city"] = email_request.city
            filters["state"] = email_request.state

        # Handle event count ranges
        if (
            email_request.hostedEventCountMin is not None
            or email_request.hostedEventCountMax is not None
        ):
            hosted_filter = {}
            if email_request.hostedEventCountMin is not None:
                hosted_filter["min"] = email_request.hostedEventCountMin
            if email_request.hostedEventCountMax is not None:
                hosted_filter["max"] = email_request.hostedEventCountMax
            filters["hostedEventCount"] = hosted_filter

        if (
            email_request.attendedEventCountMin is not None
            or email_request.attendedEventCountMax is not None
        ):
            attended_filter = {}
            if email_request.attendedEventCountMin is not None:
                attended_filter["min"] = email_request.attendedEventCountMin
            if email_request.attendedEventCountMax is not None:
                attended_filter["max"] = email_request.attendedEventCountMax
            filters["attendedEventCount"] = attended_filter

        # Get filtered users
        users = self._filter_users_for_email(filters)

        # Create UTM parameters
        utm_params = {
            "utmCampaign": email_request.utmCampaign,
            "utmSource": email_request.utmSource,
            "utmMedium": email_request.utmMedium,
        }

        # Create email analytics tracking items
        email_ids = self._create_email_analytics_items(
            users, campaign_id, email_request.subject, utm_params
        )

        # Queue background tasks for each user
        if background_tasks is None:
            background_tasks = BackgroundTasks()

        for i, user in enumerate(users):
            background_tasks.add_task(
                self.process_email_task,
                email_ids[i],
                user.email,
                email_request.subject,
                email_request.body,
                utm_params,
            )

        return EmailSendResponse(
            message=f"Emails queued successfully for {len(users)} recipients",
            emailsQueued=len(users),
            campaignId=campaign_id,
        )

    def process_email_task(
        self,
        email_id: str,
        user_email: str,
        subject: str,
        body: str,
        utm_params: Dict[str, Any],
    ):
        """Background task to process individual email sending"""
        try:
            # Send email using external service
            success = send_email_via_service(user_email, subject, body)

            if success:
                self.track_email_status(
                    email_id, "sent", sent_at=datetime.now(timezone.utc)
                )
            else:
                self.track_email_status(
                    email_id, "failed", error_message="Email service returned failure"
                )

        except Exception as e:
            self.track_email_status(email_id, "failed", error_message=str(e))

    def track_email_status(
        self,
        email_id: str,
        status: str,
        sent_at: datetime = None,
        error_message: str = None,
    ):
        """Update email status in analytics tracking"""
        try:
            update_expression = "SET #status = :status"
            expression_values = {":status": status}
            expression_names = {"#status": "status"}

            if sent_at:
                update_expression += ", sentAt = :sent_at"
                expression_values[":sent_at"] = sent_at.isoformat()

            if error_message:
                update_expression += ", errorMessage = :error_message"
                expression_values[":error_message"] = error_message

            self.table.update_item(
                Key={"PK": f"EMAIL#{email_id}", "SK": "ANALYTICS"},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ExpressionAttributeNames=expression_names,
            )

        except ClientError as e:
            print(f"Failed to update email status: {e}")

    def _filter_users_for_email(self, filters: Dict[str, Any]) -> List[UserOut]:
        """Get filtered users for email sending"""
        users, _ = self.user_service.filter_users(filters)
        return users

    def _create_email_analytics_items(
        self,
        users: List[UserOut],
        campaign_id: str,
        subject: str,
        utm_params: Dict[str, Any],
    ) -> List[str]:
        """Create email analytics tracking items for each user"""
        email_ids = []

        for user in users:
            email_id = str(uuid.uuid4())
            email_ids.append(email_id)

            analytics_item = {
                "PK": f"EMAIL#{email_id}",
                "SK": "ANALYTICS",
                "id": email_id,
                "userId": user.id,
                "email": user.email,
                "subject": subject,
                "status": "queued",
                "campaignId": campaign_id,
                "createdAt": datetime.now(timezone.utc).isoformat(),
            }

            # Add UTM parameters if provided
            if utm_params.get("utmCampaign"):
                analytics_item["utmCampaign"] = utm_params["utmCampaign"]
            if utm_params.get("utmSource"):
                analytics_item["utmSource"] = utm_params["utmSource"]
            if utm_params.get("utmMedium"):
                analytics_item["utmMedium"] = utm_params["utmMedium"]

            try:
                self.table.put_item(Item=analytics_item)
            except ClientError as e:
                print(f"Failed to create email analytics item: {e}")

        return email_ids

    def get_analytics(
        self,
        status: Optional[str] = None,
        utm_campaign: Optional[str] = None,
        utm_source: Optional[str] = None,
        utm_medium: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 50,
        last_evaluated_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get email analytics with filtering and pagination

        Args:
            status: Filter by email status (sent, failed, queued)
            utm_campaign: Filter by UTM campaign
            utm_source: Filter by UTM source
            utm_medium: Filter by UTM medium
            start_date: Filter emails created after this date (ISO format)
            end_date: Filter emails created before this date (ISO format)
            limit: Number of results per page
            last_evaluated_key: Pagination token

        Returns:
            Dict with emails, count, total, hasMore, and optional nextToken
        """

        # Build filter parameters
        filters = {}
        if status:
            filters["status"] = status
        if utm_campaign:
            filters["utmCampaign"] = utm_campaign
        if utm_source:
            filters["utmSource"] = utm_source
        if utm_medium:
            filters["utmMedium"] = utm_medium
        if start_date:
            filters["startDate"] = start_date
        if end_date:
            filters["endDate"] = end_date

        # Get filtered emails with pagination
        emails, next_token = self._get_filtered_analytics(
            filters, limit, last_evaluated_key
        )

        # Get total count for this filter
        total_count = self._get_analytics_count(filters)

        # Build response
        response = {
            "emails": [self._clean_analytics_fields(email) for email in emails],
            "count": len(emails),
            "total": total_count,
            "hasMore": next_token is not None,
        }

        if next_token:
            response["nextToken"] = next_token

        return response

    def _get_filtered_analytics(
        self,
        filters: Dict[str, Any],
        limit: int,
        last_evaluated_key: Optional[str],
    ) -> Tuple[List[Dict], Optional[str]]:
        """Get filtered email analytics with pagination"""

        emails = []
        last_db_key = None

        # Parse pagination token
        if last_evaluated_key:
            try:
                last_db_key = json.loads(base64.b64decode(last_evaluated_key).decode())
            except (ValueError, json.JSONDecodeError):
                last_db_key = None

        # Keep scanning until we have enough results
        while len(emails) < limit:
            # Get extra items to account for filtering
            scan_limit = min((limit - len(emails)) * 5, 100)

            # Build scan parameters
            scan_params = {
                "FilterExpression": Attr("SK").eq("ANALYTICS"),
                "Limit": scan_limit,
            }

            if last_db_key:
                scan_params["ExclusiveStartKey"] = last_db_key

            # Execute scan
            response = self.table.scan(**scan_params)

            # Filter results
            batch_items = []
            for item in response.get("Items", []):
                if self._matches_analytics_filters(item, filters):
                    batch_items.append(item)

            # Add filtered items to result
            emails.extend(batch_items)

            # Update last evaluated key for next iteration
            last_db_key = response.get("LastEvaluatedKey")

            # If no more data in DB, stop
            if not last_db_key:
                break

        # Sort by creation date (newest first)
        emails.sort(key=lambda x: x.get("createdAt", ""), reverse=True)

        # Take only the requested limit
        result_emails = emails[:limit]

        # Determine if there are more results
        has_more = len(emails) > limit or last_db_key is not None

        # Encode pagination token if there are more results
        next_token = None
        if has_more and last_db_key:
            next_token = base64.b64encode(json.dumps(last_db_key).encode()).decode()

        return result_emails, next_token

    def _matches_analytics_filters(self, item: Dict, filters: Dict[str, Any]) -> bool:
        """Check if analytics item matches all filter criteria"""

        # Status filter
        if "status" in filters:
            if item.get("status") != filters["status"]:
                return False

        # UTM campaign filter
        if "utmCampaign" in filters:
            if item.get("utmCampaign") != filters["utmCampaign"]:
                return False

        # UTM source filter
        if "utmSource" in filters:
            if item.get("utmSource") != filters["utmSource"]:
                return False

        # UTM medium filter
        if "utmMedium" in filters:
            if item.get("utmMedium") != filters["utmMedium"]:
                return False

        # Date range filters
        if "startDate" in filters or "endDate" in filters:
            item_date = item.get("createdAt")
            if not item_date:
                return False

            try:
                # Parse item creation date - handle both Z and +00:00 formats
                clean_item_date = item_date
                if clean_item_date.endswith("Z"):
                    clean_item_date = clean_item_date[:-1] + "+00:00"
                item_datetime = datetime.fromisoformat(clean_item_date)

                # Check start date
                if "startDate" in filters:
                    start_date = filters["startDate"]
                    clean_start_date = start_date
                    if clean_start_date.endswith("Z"):
                        clean_start_date = clean_start_date[:-1] + "+00:00"
                    start_datetime = datetime.fromisoformat(clean_start_date)
                    if item_datetime < start_datetime:
                        return False

                # Check end date
                if "endDate" in filters:
                    end_date = filters["endDate"]
                    clean_end_date = end_date
                    if clean_end_date.endswith("Z"):
                        clean_end_date = clean_end_date[:-1] + "+00:00"
                    end_datetime = datetime.fromisoformat(clean_end_date)
                    if item_datetime > end_datetime:
                        return False

            except (ValueError, TypeError):
                # Date parsing failed, exclude this item
                return False

        return True

    def _get_analytics_count(self, filters: Dict[str, Any]) -> int:
        """Get total count of emails matching filters"""

        count = 0
        last_evaluated_key = None

        while True:
            scan_params = {
                "FilterExpression": Attr("SK").eq("ANALYTICS"),
                "Select": "COUNT",
            }

            if last_evaluated_key:
                scan_params["ExclusiveStartKey"] = last_evaluated_key

            response = self.table.scan(**scan_params)

            # For count queries, we need to manually check filters
            # since DynamoDB COUNT doesn't work well with complex filters
            if not filters:
                count += response["Count"]
            else:
                # Get actual items to apply our custom filters
                scan_params["Select"] = "ALL_ATTRIBUTES"
                items_response = self.table.scan(**scan_params)
                for item in items_response.get("Items", []):
                    if self._matches_analytics_filters(item, filters):
                        count += 1

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        return count

    def _clean_analytics_fields(self, item: Dict) -> Dict:
        """Clean analytics item for API response"""
        cleaned = {k: v for k, v in item.items() if k not in ["PK", "SK"]}

        # Ensure all expected fields are present
        if "utmCampaign" not in cleaned:
            cleaned["utmCampaign"] = None
        if "utmSource" not in cleaned:
            cleaned["utmSource"] = None
        if "utmMedium" not in cleaned:
            cleaned["utmMedium"] = None
        if "sentAt" not in cleaned:
            cleaned["sentAt"] = None
        if "errorMessage" not in cleaned:
            cleaned["errorMessage"] = None

        return cleaned

    def _get_all_email_analytics(self) -> List[Dict[str, Any]]:
        """Helper method to get all email analytics items (for testing)"""
        all_items = []
        last_evaluated_key = None

        while True:
            scan_params = {
                "FilterExpression": Attr("SK").eq("ANALYTICS"),
            }

            if last_evaluated_key:
                scan_params["ExclusiveStartKey"] = last_evaluated_key

            response = self.table.scan(**scan_params)
            all_items.extend(response.get("Items", []))

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        return all_items

    def _get_email_analytics(self, email_id: str) -> Dict[str, Any]:
        """Get email analytics item by ID"""
        try:
            response = self.table.get_item(
                Key={"PK": f"EMAIL#{email_id}", "SK": "ANALYTICS"}
            )
            return response.get("Item", {})
        except ClientError as e:
            print(f"Failed to get email analytics: {e}")
            return {}
