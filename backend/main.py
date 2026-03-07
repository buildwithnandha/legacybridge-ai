"""
LegacyBridge AI — FastAPI Entry Point.

Mounts all routers and configures CORS for the React frontend.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers.health import router as health_router
from routers.pipeline import router as pipeline_router
from routers.recon import router as recon_router

app = FastAPI(
    title="LegacyBridge AI",
    description="AI-Powered Data Migration Reconciliation & Root Cause Analysis",
    version="1.0.0",
)

# CORS — allow React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(pipeline_router)
app.include_router(recon_router)


@app.get("/")
def root():
    return {
        "name": "LegacyBridge AI",
        "version": "1.0.0",
        "docs": "/docs",
    }
