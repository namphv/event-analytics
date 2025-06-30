import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any
from fastapi import BackgroundTasks
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
