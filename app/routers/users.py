from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
from app.schemas.user import UserCreate, UserOut
from app.services.user_service import UserService
from app.database.dynamodb import get_db_connection

router = APIRouter(prefix="/users", tags=["users"])


def get_user_service():
    """Dependency to get UserService instance"""
    db = get_db_connection()
    return UserService(db)


@router.post("/", response_model=UserOut, status_code=201)
async def create_user(
    user_data: UserCreate, user_service: UserService = Depends(get_user_service)
):
    """Create a new user"""
    try:
        return user_service.create_user(user_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=list[UserOut])
async def filter_users(
    company: Optional[str] = Query(None, description="Filter by company"),
    jobTitle: Optional[str] = Query(None, description="Filter by job title"),
    city: Optional[str] = Query(None, description="Filter by city"),
    state: Optional[str] = Query(None, description="Filter by state"),
    hostedEventCountMin: Optional[int] = Query(
        None, description="Minimum hosted events"
    ),
    hostedEventCountMax: Optional[int] = Query(
        None, description="Maximum hosted events"
    ),
    attendedEventCountMin: Optional[int] = Query(
        None, description="Minimum attended events"
    ),
    attendedEventCountMax: Optional[int] = Query(
        None, description="Maximum attended events"
    ),
    sort: Optional[str] = Query("lastName", description="Sort field"),
    order: Optional[str] = Query("asc", description="Sort order (asc/desc)"),
    user_service: UserService = Depends(get_user_service),
):
    """Filter users based on criteria"""
    try:
        filters = {}

        if company:
            filters["company"] = company
        if jobTitle:
            filters["jobTitle"] = jobTitle
        if city and state:
            filters["city"] = city
            filters["state"] = state

        # Handle event count ranges
        if hostedEventCountMin is not None or hostedEventCountMax is not None:
            hosted_filter = {}
            if hostedEventCountMin is not None:
                hosted_filter["min"] = hostedEventCountMin
            if hostedEventCountMax is not None:
                hosted_filter["max"] = hostedEventCountMax
            filters["hostedEventCount"] = hosted_filter

        if attendedEventCountMin is not None or attendedEventCountMax is not None:
            attended_filter = {}
            if attendedEventCountMin is not None:
                attended_filter["min"] = attendedEventCountMin
            if attendedEventCountMax is not None:
                attended_filter["max"] = attendedEventCountMax
            filters["attendedEventCount"] = attended_filter

        # Add sorting
        filters["sort"] = sort
        filters["order"] = order

        return user_service.filter_users(filters)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
