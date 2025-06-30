from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from typing import Optional, Dict, Any
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


@router.get("/analytics", response_model=Dict[str, Any])
async def get_email_analytics(
    status: Optional[str] = Query(
        None, description="Filter by email status (sent, failed, queued)"
    ),
    utm_campaign: Optional[str] = Query(None, description="Filter by UTM campaign"),
    utm_source: Optional[str] = Query(None, description="Filter by UTM source"),
    utm_medium: Optional[str] = Query(None, description="Filter by UTM medium"),
    start_date: Optional[str] = Query(
        None, description="Filter emails created after this date (ISO format)"
    ),
    end_date: Optional[str] = Query(
        None, description="Filter emails created before this date (ISO format)"
    ),
    limit: int = Query(50, ge=1, le=100, description="Number of results per page"),
    nextToken: Optional[str] = Query(
        None, description="Pagination token from previous response"
    ),
    email_service: EmailService = Depends(get_email_service),
):
    """Get email analytics with filtering and pagination"""
    try:
        return email_service.get_analytics(
            status=status,
            utm_campaign=utm_campaign,
            utm_source=utm_source,
            utm_medium=utm_medium,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            last_evaluated_key=nextToken,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
