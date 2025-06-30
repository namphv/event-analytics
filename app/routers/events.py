from fastapi import APIRouter, HTTPException, Depends
from app.schemas.event import EventCreate, EventOut
from app.services.event_service import EventService
from app.database.dynamodb import get_db_connection

router = APIRouter(prefix="/events", tags=["events"])


def get_event_service():
    """Dependency to get EventService instance"""
    db = get_db_connection()
    return EventService(db)


@router.post("/", response_model=EventOut, status_code=201)
async def create_event(
    event_data: EventCreate, event_service: EventService = Depends(get_event_service)
):
    """Create a new event with hosts and attendees"""
    try:
        return event_service.create_event(event_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
