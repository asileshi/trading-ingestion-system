from fastapi import FastAPI
from .routes import router
from pydantic import BaseModel

app = FastAPI(title="Trading Backend")

app.include_router(router)

