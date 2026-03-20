from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from database import get_db
from utils import get_proxy_config

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    phone: str
    code: str
    phone_code_hash: str
    api_id: int = 35019294
    api_hash: str = "9e2d91fe6876d834bae4707b0875e2d7"
    password: str | None = None
    temp_session: str | None = None
    as_manager: bool = False

class SendCodeRequest(BaseModel):
    phone: str
    api_id: int = 35019294
    api_hash: str = "9e2d91fe6876d834bae4707b0875e2d7"

@router.post("/send_code")
async def send_code(req: SendCodeRequest):
    # Determine if we should rotate keys
    should_rotate = (req.api_id == 35019294)
    
    max_retries = 10 if should_rotate else 1
    last_error = None
    
    for attempt in range(max_retries):
        if should_rotate:
            from apikeys import get_next_api_key
            key = await get_next_api_key()
            if key:
                req.api_id = key["api_id"]
                req.api_hash = key["api_hash"]
            else:
                pass
        
        proxy = await get_proxy_config()
        # IMPORTANT: Use a session file instead of StringSession() to persist the auth state
        # between send_code and login.
        import os
        from database import SESSION_DIR
        session_name = f"login_temp_{req.phone.replace(' ', '').replace('+', '')}"
        session_path = os.path.join(SESSION_DIR, session_name)
        
        client = TelegramClient(session_path, req.api_id, req.api_hash, proxy=proxy)
        
        try:
            await client.connect()
            sent = await client.send_code_request(req.phone)
            # We don't disconnect here immediately if we want to keep the session alive?
            # Actually, Telethon saves the session to disk, so we can disconnect.
            
            return {
                "phone_code_hash": sent.phone_code_hash,
                "api_id": req.api_id,
                "api_hash": req.api_hash,
                "temp_session": session_name # Tell frontend/login which session file to use
            }
        except (errors.ApiIdInvalidError, errors.ApiIdPublishedFloodError, errors.PhoneNumberInvalidError) as e:
            last_error = str(e)
            print(f"Attempt {attempt+1} failed with API ID {req.api_id}: {e}")
            # Only retry if it's an API ID issue (or we are rotating)
            # PhoneNumberInvalidError means phone is bad, no point rotating, but let's keep logic simple
            if not should_rotate:
                raise HTTPException(status_code=400, detail=last_error)
            # If we are rotating, we continue to next loop iteration
        except Exception as e:
            last_error = str(e)
            print(f"Attempt {attempt+1} failed with unexpected error: {e}")
            # If we are rotating, we continue to next loop iteration
        finally:
            if client:
                await client.disconnect()

    raise HTTPException(status_code=400, detail=f"Failed after {max_retries} attempts. Last error: {last_error}")

@router.post("/login")
async def login(req: LoginRequest):
    proxy = await get_proxy_config()
    import os
    from database import SESSION_DIR
    
    # Use the temp session from send_code if available
    if req.temp_session:
        session_path = os.path.join(SESSION_DIR, req.temp_session)
        client = TelegramClient(session_path, req.api_id, req.api_hash, proxy=proxy)
    else:
        # Fallback to string session (legacy behavior)
        client = TelegramClient(StringSession(), req.api_id, req.api_hash, proxy=proxy)
        
    await client.connect()

    try:
        # Check if already authorized (should not be, but good check)
        if not await client.is_user_authorized():
            try:
                # Direct sign_in with code
                await client.sign_in(req.phone, req.code, phone_code_hash=req.phone_code_hash)
            except errors.SessionPasswordNeededError:
                if not req.password:
                    raise HTTPException(status_code=400, detail="This account requires 2FA password")
                await client.sign_in(password=req.password)
            except errors.PhoneCodeExpiredError:
                 raise HTTPException(status_code=400, detail="验证码已过期，请重新发送")
            except errors.PhoneCodeInvalidError:
                 raise HTTPException(status_code=400, detail="验证码错误")
        
        # Check if login was actually successful
        me = await client.get_me()
        if not me:
             raise HTTPException(status_code=400, detail="Login failed, could not get user info")
             
        # If we used a temp session file, we should keep using it.
        # client.session.save() for SQLiteSession returns None or persists to file.
        # StringSession.save() returns the string.
        
        final_session_string = None
        final_session_file = None
        
        if req.temp_session:
            # We are using a file session. Rename it to permanent name.
            # Usually we use phone number as filename, or keep it as is.
            # Let's rename to {phone}.session to match other sessions convention
            # But wait, other sessions use session_file column.
            
            clean_phone = req.phone.replace(" ", "").replace("+", "")
            new_filename = f"{clean_phone}.session"
            new_path = os.path.join(SESSION_DIR, new_filename)
            
            # Close connection before moving file?
            await client.disconnect()
            
            # Move temp file to new path
            if os.path.exists(session_path):
                # If target exists, remove it first
                if os.path.exists(new_path):
                    os.remove(new_path)
                os.rename(session_path, new_path)
                final_session_file = new_filename
            else:
                # Should not happen
                final_session_file = req.temp_session
        else:
            # Legacy string session
            final_session_string = client.session.save()
        
        async with get_db() as db:
            existing = await db.execute("SELECT id FROM sessions WHERE phone = ? ORDER BY id DESC LIMIT 1", (req.phone,))
            row = await existing.fetchone()
            if row:
                await db.execute(
                    "UPDATE sessions SET api_id = ?, api_hash = ?, session_string = ?, session_file = ?, status = 'active', flood_wait = NULL, is_manager = ? WHERE id = ?",
                    (req.api_id, req.api_hash, final_session_string, final_session_file, 1 if req.as_manager else 0, row[0]),
                )
            else:
                await db.execute(
                    "INSERT INTO sessions (phone, api_id, api_hash, session_string, session_file, status, is_manager) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (req.phone, req.api_id, req.api_hash, final_session_string, final_session_file, "active", 1 if req.as_manager else 0)
                )
            await db.commit()
            
        return {"status": "success", "phone": req.phone, "is_manager": req.as_manager}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        if client and client.is_connected():
            await client.disconnect()
