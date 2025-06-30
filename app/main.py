from fastapi import FastAPI
from .database.dynamodb import get_db_connection
from .routers import users, events

app = FastAPI(title="CRM Event Analytics API", version="1.0.0")

db_connection = get_db_connection()

# Include routers
app.include_router(users.router)
app.include_router(events.router)


@app.get("/")
def read_root():
    return {"message": "CRM Event Analytics API", "status": "running"}
