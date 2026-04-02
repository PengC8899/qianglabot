from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import JSONResponse
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

_public_paths = {
    "/",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/auth/admin_login",
}

def _extract_bearer_token(request: Request):
    header = request.headers.get("authorization") or request.headers.get("Authorization")
    if header and header.lower().startswith("bearer "):
        return header[7:].strip()
    return None

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if request.method == "OPTIONS":
        return await call_next(request)
    if path in _public_paths:
        return await call_next(request)

    token = _extract_bearer_token(request)
    if not token:
        token = request.query_params.get("token")
    if not auth.is_admin_token_valid(token):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)

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
