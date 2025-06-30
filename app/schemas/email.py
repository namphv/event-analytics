from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class EmailSendRequest(BaseModel):
    """Request schema for sending bulk emails"""

    subject: str
    body: str
    utmCampaign: Optional[str] = None
    utmSource: Optional[str] = "crm"
    utmMedium: Optional[str] = "email"

    # User filter criteria (same as user filtering)
    company: Optional[str] = None
    jobTitle: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    hostedEventCountMin: Optional[int] = None
    hostedEventCountMax: Optional[int] = None
    attendedEventCountMin: Optional[int] = None
    attendedEventCountMax: Optional[int] = None


class EmailSendResponse(BaseModel):
    """Response schema for email send operation"""

    message: str
    emailsQueued: int
    campaignId: str


class EmailAnalytics(BaseModel):
    """Email analytics data"""

    id: str
    userId: str
    email: str
    subject: str
    status: str  # 'queued', 'sent', 'failed', 'bounced'
    sentAt: Optional[datetime] = None
    utmCampaign: Optional[str] = None
    utmSource: Optional[str] = None
    utmMedium: Optional[str] = None
    errorMessage: Optional[str] = None
