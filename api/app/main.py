from fastapi import FastAPI
from .routes import router

app = FastAPI(title="Trading Backend")

app.include_router(router)
