from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class EventBase(BaseModel):
    slug: str
    title: str
    description: str
    startAt: datetime
    endAt: datetime
    venue: str
    maxCapacity: int
    owner: str


class EventCreate(EventBase):
    hostIds: Optional[List[str]] = []
    attendeeIds: Optional[List[str]] = []


class EventOut(EventBase):
    id: str
    attendeeCount: int
