from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from app.schemas.email import EmailSendRequest, EmailSendResponse
from app.services.email_service import EmailService
from app.database.dynamodb import get_db_connection

router = APIRouter(prefix="/emails", tags=["emails"])


def get_email_service():
    """Dependency to get EmailService instance"""
    db = get_db_connection()
    return EmailService(db)


@router.post("/send", response_model=EmailSendResponse, status_code=200)
async def send_bulk_email(
    email_request: EmailSendRequest,
    background_tasks: BackgroundTasks,
    email_service: EmailService = Depends(get_email_service),
):
    """Send emails to filtered users in background"""
    try:
        return email_service.send_bulk_email(email_request, background_tasks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
