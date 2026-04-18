from dotenv import load_dotenv
load_dotenv()  # must be before any imports that read env vars

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
import os

from rate_limit import limiter
from routes.trades import router as trades_router
from routes.import_trades import router as import_router
from routes.payments import router as payments_router
from routes.waitlist import router as waitlist_router

app = FastAPI(title="Tradfy API", version="1.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── CORS ──────────────────────────────────────────────────────────────────────
ALLOWED_ORIGINS = [
    "https://tradfy-frontend.vercel.app",
    "http://localhost:3000",
    "http://localhost:3001",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(trades_router)
app.include_router(import_router)
app.include_router(payments_router)
app.include_router(waitlist_router)


@app.get("/")
def health():
    return {"status": "ok", "service": "tradfy-backend"}
