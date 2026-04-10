from dotenv import load_dotenv
load_dotenv()  # must be before any imports that read env vars

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

from routes.trades import router as trades_router
from routes.import_trades import router as import_router

app = FastAPI(title="Tradfy API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        os.getenv("FRONTEND_URL", "http://localhost:3000"),
        "http://localhost:3000",
        "http://localhost:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(trades_router)
app.include_router(import_router)


@app.get("/")
def health():
    return {"status": "ok", "service": "tradfy-backend"}
