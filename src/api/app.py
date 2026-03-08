"""FastAPI application entry point.

Start the server::

    uvicorn src.api.app:app --reload

The app mounts the v1 API router from ``src.api.routes`` and adds
CORS middleware so any frontend origin can call it during development.
"""

import logging
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from src.api.admin import router as admin_router
from src.api.registration import router as registration_router
from src.api.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)

app = FastAPI(
    title="Football Fam API",
    description=(
        "Player marketplace data for English lower league football "
        "(Steps 1-6 of the pyramid)."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(registration_router)
app.include_router(admin_router)

_upload_dir = Path(os.getenv("UPLOAD_DIR", "data/uploads"))
_upload_dir.mkdir(parents=True, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=str(_upload_dir)), name="uploads")


@app.get("/health", tags=["system"])
def health_check() -> dict:
    """Liveness probe."""
    return {"status": "ok"}
