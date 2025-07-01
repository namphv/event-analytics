from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .database.dynamodb import get_db_connection
from .routers import users, events, emails

app = FastAPI(
    title="CRM Event Analytics API",
    version="1.0.0",
    description="A comprehensive CRM system with event analytics and email campaigns",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Configure CORS for docs UI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db_connection = get_db_connection()

# Include routers
app.include_router(users.router)
app.include_router(events.router)
app.include_router(emails.router)


@app.get("/")
def read_root():
    return {"message": "CRM Event Analytics API", "status": "running"}
