import uuid
from app.schemas.event import EventCreate, EventOut
from app.services.user_service import UserService
from botocore.exceptions import ClientError


class EventService:
    def __init__(self, dynamodb_resource, table_name="CommunityApp"):
        self.dynamodb = dynamodb_resource
        self.table = dynamodb_resource.Table(table_name)
        self.user_service = UserService(dynamodb_resource, table_name)

    def create_event(self, event_data: EventCreate) -> EventOut:
        """Create event with hosts and attendees in a transaction"""
        event_id = str(uuid.uuid4())

        # Prepare transaction items
        transact_items = []

        # 1. Create the main Event Detail item
        event_item = {
            "PK": f"EVENT#{event_id}",
            "SK": "DETAIL",
            "id": event_id,
            "slug": event_data.slug,
            "title": event_data.title,
            "description": event_data.description,
            "startAt": event_data.startAt.isoformat(),
            "endAt": event_data.endAt.isoformat(),
            "venue": event_data.venue,
            "maxCapacity": event_data.maxCapacity,
            "owner": event_data.owner,
            "attendeeCount": len(event_data.attendeeIds),
        }

        # Add GSI attributes for event analytics
        event_item["GSI_EventsByDate_PK"] = "EVENT_TIMELINE"
        event_item[
            "GSI_EventsByDate_SK"
        ] = f"DATE#{event_data.startAt.date().isoformat()}"

        transact_items.append(
            {"Put": {"TableName": self.table.table_name, "Item": event_item}}
        )

        # 2. Create Event Host items and increment hosted counts
        for host_id in event_data.hostIds:
            # Create host relationship item
            host_item = {
                "PK": f"EVENT#{event_id}",
                "SK": f"HOST#{host_id}",
                "userId": host_id,
                "eventId": event_id,
                "role": "host",
            }

            # Add GSI attribute for finding event attendees
            host_item["GSI_EventAttendees_PK"] = f"EVENT#{event_id}"
            host_item["GSI_EventAttendees_SK"] = f"USER#{host_id}"

            transact_items.append(
                {
                    "Put": {
                        "TableName": self.table.table_name,
                        "Item": host_item,
                        "ConditionExpression": "attribute_not_exists(PK)",
                    }
                }
            )

            # Verify user exists (will be incremented after transaction)
            transact_items.append(
                {
                    "ConditionCheck": {
                        "TableName": self.table.table_name,
                        "Key": {"PK": f"USER#{host_id}", "SK": "PROFILE"},
                        "ConditionExpression": "attribute_exists(PK)",  # User must exist
                    }
                }
            )

        # 3. Create Event Attendee items and increment attended counts
        for attendee_id in event_data.attendeeIds:
            # Create attendee relationship item
            attendee_item = {
                "PK": f"USER#{attendee_id}",
                "SK": f"ATTENDS#{event_id}",
                "userId": attendee_id,
                "eventId": event_id,
                "role": "attendee",
            }

            # Add GSI attribute for finding event attendees
            attendee_item["GSI_EventAttendees_PK"] = f"EVENT#{event_id}"
            attendee_item["GSI_EventAttendees_SK"] = f"USER#{attendee_id}"

            transact_items.append(
                {
                    "Put": {
                        "TableName": self.table.table_name,
                        "Item": attendee_item,
                        "ConditionExpression": "attribute_not_exists(PK)",
                    }
                }
            )

            # Verify user exists (will be incremented after transaction)
            transact_items.append(
                {
                    "ConditionCheck": {
                        "TableName": self.table.table_name,
                        "Key": {"PK": f"USER#{attendee_id}", "SK": "PROFILE"},
                        "ConditionExpression": "attribute_exists(PK)",  # User must exist
                    }
                }
            )

        try:
            # Execute transaction
            if transact_items:
                self.dynamodb.meta.client.transact_write_items(
                    TransactItems=transact_items
                )

            # After successful transaction, increment user counts with proper GSI updates
            for host_id in event_data.hostIds:
                self.user_service.increment_hosted_count(host_id)

            for attendee_id in event_data.attendeeIds:
                self.user_service.increment_attended_count(attendee_id)

            # Return the event object
            return EventOut(
                id=event_id,
                slug=event_data.slug,
                title=event_data.title,
                description=event_data.description,
                startAt=event_data.startAt,
                endAt=event_data.endAt,
                venue=event_data.venue,
                maxCapacity=event_data.maxCapacity,
                owner=event_data.owner,
                attendeeCount=len(event_data.attendeeIds),
            )

        except ClientError as e:
            if e.response["Error"]["Code"] == "TransactionCanceledException":
                # Check cancellation reasons
                cancellation_reasons = e.response.get("CancellationReasons", [])
                for reason in cancellation_reasons:
                    if reason.get("Code") == "ConditionalCheckFailed":
                        raise Exception(
                            "Transaction failed: User not found or duplicate entry"
                        )
                raise Exception("Transaction failed: Conditional check failed")
            else:
                raise Exception(f"Failed to create event: {e}")
