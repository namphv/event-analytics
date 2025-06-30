from pydantic import BaseModel, EmailStr


class UserBase(BaseModel):
    firstName: str
    lastName: str
    phoneNumber: str
    email: EmailStr
    avatar: str | None = None
    gender: str | None = None
    jobTitle: str | None = None
    company: str | None = None
    city: str | None = None
    state: str | None = None


class UserCreate(UserBase):
    pass


class UserOut(UserBase):
    id: str
    hostedEventCount: int
    attendedEventCount: int
