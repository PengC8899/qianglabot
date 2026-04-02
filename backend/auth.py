import os
import secrets
import time
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from database import get_db
from utils import get_proxy_config

router = APIRouter(prefix="/auth", tags=["auth"])

_admin_tokens = {}
_admin_token_ttl = int(os.environ.get("ADMIN_TOKEN_TTL_SECONDS", "43200"))
_admin_username = os.environ.get("ADMIN_USERNAME", "admin")
_admin_password = os.environ.get("ADMIN_PASSWORD", "9999")

def _cleanup_admin_tokens():
    now_ts = int(time.time())
    expired = [token for token, exp in _admin_tokens.items() if exp <= now_ts]
    for token in expired:
        _admin_tokens.pop(token, None)

def create_admin_token():
    _cleanup_admin_tokens()
    token = secrets.token_urlsafe(32)
    _admin_tokens[token] = int(time.time()) + _admin_token_ttl
    return token

def is_admin_token_valid(token: str | None):
    if not token:
        return False
    _cleanup_admin_tokens()
    expires_at = _admin_tokens.get(token)
    if not expires_at:
        return False
    return expires_at > int(time.time())

class AdminLoginRequest(BaseModel):
    username: str
    password: str


class LoginRequest(BaseModel):
    phone: str
    code: str
    phone_code_hash: str
    api_id: int | None = None
    api_hash: str | None = None
    password: str | None = None
    temp_session: str | None = None
    as_manager: bool = False

class SendCodeRequest(BaseModel):
    phone: str
    api_id: int | None = None
    api_hash: str | None = None

@router.post("/admin_login")
async def admin_login(req: AdminLoginRequest):
    if req.username != _admin_username or req.password != _admin_password:
        raise HTTPException(status_code=401, detail="账号或密码错误")
    token = create_admin_token()
    return {"token": token, "expires_in": _admin_token_ttl}

async def resolve_api_credentials(api_id: int | None, api_hash: str | None):
    if api_id and api_hash:
        return int(api_id), api_hash

    from apikeys import get_next_api_key
    key = await get_next_api_key()
    if key:
        return int(key["api_id"]), key["api_hash"]

    env_api_id = os.environ.get("DEFAULT_API_ID")
    env_api_hash = os.environ.get("DEFAULT_API_HASH")
    if env_api_id and env_api_hash:
        try:
            return int(env_api_id), env_api_hash
        except Exception:
            pass

    raise HTTPException(status_code=400, detail="缺少可用 API 凭据，请在 API Key 管理中添加或设置环境变量 DEFAULT_API_ID/DEFAULT_API_HASH")

@router.post("/send_code")
async def send_code(req: SendCodeRequest):
    max_retries = 10 if not (req.api_id and req.api_hash) else 1
    last_error = None
    
    for attempt in range(max_retries):
        resolved_api_id, resolved_api_hash = await resolve_api_credentials(req.api_id, req.api_hash)
        
        from utils import get_proxy_config, get_device_fingerprint
        proxy = await get_proxy_config()
        fp = get_device_fingerprint()
        import os
        from database import SESSION_DIR
        session_name = f"login_temp_{req.phone.replace(' ', '').replace('+', '')}"
        session_path = os.path.join(SESSION_DIR, session_name)
        
        client = TelegramClient(session_path, resolved_api_id, resolved_api_hash, proxy=proxy, **fp)
        
        try:
            await client.connect()
            sent = await client.send_code_request(req.phone)
            
            return {
                "phone_code_hash": sent.phone_code_hash,
                "api_id": resolved_api_id,
                "api_hash": resolved_api_hash,
                "temp_session": session_name
            }
        except (errors.ApiIdInvalidError, errors.ApiIdPublishedFloodError, errors.PhoneNumberInvalidError) as e:
            last_error = str(e)
            print(f"Attempt {attempt+1} failed with API ID {resolved_api_id}: {e}")
            if req.api_id and req.api_hash:
                raise HTTPException(status_code=400, detail=last_error)
        except Exception as e:
            last_error = str(e)
            print(f"Attempt {attempt+1} failed with unexpected error: {e}")
        finally:
            if client:
                await client.disconnect()

    raise HTTPException(status_code=400, detail=f"Failed after {max_retries} attempts. Last error: {last_error}")

@router.post("/login")
async def login(req: LoginRequest):
    resolved_api_id, resolved_api_hash = await resolve_api_credentials(req.api_id, req.api_hash)
    from utils import get_proxy_config, get_device_fingerprint
    proxy = await get_proxy_config()
    fp = get_device_fingerprint()
    import os
    from database import SESSION_DIR
    
    # Use the temp session from send_code if available
    if req.temp_session:
        session_path = os.path.join(SESSION_DIR, req.temp_session)
        client = TelegramClient(session_path, resolved_api_id, resolved_api_hash, proxy=proxy, **fp)
    else:
        client = TelegramClient(StringSession(), resolved_api_id, resolved_api_hash, proxy=proxy, **fp)
        
    await client.connect()

    try:
        if not await client.is_user_authorized():
            try:
                await client.sign_in(req.phone, req.code, phone_code_hash=req.phone_code_hash)
            except errors.SessionPasswordNeededError:
                if not req.password:
                    raise HTTPException(status_code=400, detail="This account requires 2FA password")
                await client.sign_in(password=req.password)
            except errors.PhoneCodeExpiredError:
                 raise HTTPException(status_code=400, detail="验证码已过期，请重新发送")
            except errors.PhoneCodeInvalidError:
                 raise HTTPException(status_code=400, detail="验证码错误")
        
        me = await client.get_me()
        if not me:
             raise HTTPException(status_code=400, detail="Login failed, could not get user info")
        
        final_session_string = None
        final_session_file = None
        
        if req.temp_session:
            clean_phone = req.phone.replace(" ", "").replace("+", "")
            new_filename = f"{clean_phone}.session"
            new_path = os.path.join(SESSION_DIR, new_filename)
            
            await client.disconnect()
            
            if os.path.exists(session_path):
                if os.path.exists(new_path):
                    os.remove(new_path)
                os.rename(session_path, new_path)
                final_session_file = new_filename
            else:
                final_session_file = req.temp_session
        else:
            final_session_string = client.session.save()
        
        async with get_db() as db:
            existing = await db.execute("SELECT id FROM sessions WHERE phone = ? ORDER BY id DESC LIMIT 1", (req.phone,))
            row = await existing.fetchone()
            if row:
                await db.execute(
                    "UPDATE sessions SET api_id = ?, api_hash = ?, session_string = ?, session_file = ?, status = 'active', flood_wait = NULL, is_manager = ? WHERE id = ?",
                    (resolved_api_id, resolved_api_hash, final_session_string, final_session_file, 1 if req.as_manager else 0, row[0]),
                )
            else:
                await db.execute(
                    "INSERT INTO sessions (phone, api_id, api_hash, session_string, session_file, status, is_manager) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (req.phone, resolved_api_id, resolved_api_hash, final_session_string, final_session_file, "active", 1 if req.as_manager else 0)
                )
            await db.commit()
            
        return {"status": "success", "phone": req.phone, "is_manager": req.as_manager}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        if client and client.is_connected():
            await client.disconnect()
