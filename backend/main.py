from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import init_db
import auth
import sessions
import tasks
import logs
import blacklist
import proxies
import apikeys
import invite_system

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(sessions.router)
app.include_router(tasks.router)
app.include_router(invite_system.router)
app.include_router(logs.router, prefix="/logs", tags=["logs"])
app.include_router(blacklist.router, prefix="/blacklist", tags=["blacklist"])
app.include_router(proxies.router, prefix="/proxies", tags=["proxies"])
app.include_router(apikeys.router, prefix="/apikeys", tags=["apikeys"])

@app.on_event("startup")
async def startup_event():
    await init_db()

@app.get("/")
async def root():
    return {"message": "Telegram Bot API is running. Visit /docs for documentation."}
