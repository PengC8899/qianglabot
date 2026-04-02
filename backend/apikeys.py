from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from database import fetch_all, execute, now_iso
import random
from worker import build_client_from_session

router = APIRouter()

async def get_next_api_key():
    # Rotate keys by picking the one least recently used (LRU)
    # NULL last_used_at comes first (never used)
    # We update last_used_at immediately to mark it as 'just used'
    keys = await fetch_all("SELECT * FROM api_keys ORDER BY last_used_at ASC LIMIT 1")
    if keys:
        key = keys[0]
        # Update usage time
        await execute("UPDATE api_keys SET last_used_at = ? WHERE id = ?", (now_iso(), key["id"]))
        return key
    return None

class ApiKeyCreate(BaseModel):
    lines: str # format: api_id:api_hash per line

class BatchIds(BaseModel):
    ids: list[int]

@router.get("")
async def list_keys():
    rows = await fetch_all("SELECT * FROM api_keys ORDER BY id DESC")
    return {"items": rows}

@router.post("/add")
async def add_keys(payload: ApiKeyCreate):
    lines = payload.lines.split("\n")
    count = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            # Try splitting by various separators
            if ":" in line:
                parts = line.split(":")
            elif "|" in line:
                parts = line.split("|")
            elif "," in line:
                parts = line.split(",")
            else:
                parts = line.split()
            
            if len(parts) >= 2:
                api_id = int(parts[0].strip())
                api_hash = parts[1].strip()
                
                # Check if exists
                exists = await fetch_all("SELECT id FROM api_keys WHERE api_id = ?", (api_id,))
                if not exists:
                    await execute(
                        "INSERT INTO api_keys (api_id, api_hash, created_at) VALUES (?, ?, ?)",
                        (api_id, api_hash, now_iso())
                    )
                    count += 1
        except:
            continue
            
    return {"status": "success", "added": count}

@router.delete("/{id}")
async def delete_key(id: int):
    await execute("DELETE FROM api_keys WHERE id = ?", (id,))
    return {"status": "deleted"}

@router.post("/batch_check")
async def batch_check_keys(payload: BatchIds):
    ids = payload.ids
    results = []
    
    # Get a valid session
    session_row = await get_valid_tester_session()
    
    if not session_row:
        return {"results": [{"id": i, "status": "error", "error": "需要至少一个活跃且可用的账号"} for i in ids]}
        
    for kid in ids:
        try:
            # We pass the pre-validated session to check_key to avoid re-fetching
            # But check_key currently fetches it inside.
            # Let's refactor check_key to accept an optional session
            res = await check_key_logic(kid, session_row)
            results.append({"id": kid, "status": res["status"], "error": res.get("error")})
        except Exception as e:
            results.append({"id": kid, "status": "error", "error": str(e)})
            
    return {"results": results}

@router.post("/check/{id}")
async def check_key_endpoint(id: int):
    session_row = await get_valid_tester_session()
    if not session_row:
         return {"status": "error", "error": "需要至少一个活跃且可用的账号来检测 API Key"}
    return await check_key_logic(id, session_row)

async def get_valid_tester_session():
    rows = await fetch_all("SELECT * FROM sessions WHERE status = 'active' ORDER BY session_string DESC, id ASC")
    
    for row in rows:
        client = None
        try:
            client = await build_client_from_session(row, use_rotating_api=False)
            await client.connect()
            if await client.is_user_authorized():
                await client.disconnect()
                return row
            else:
                await execute("UPDATE sessions SET status = 'invalid' WHERE id = ?", (row["id"],))
        except Exception:
             pass
        finally:
            if client and client.is_connected():
                await client.disconnect()
                
    return None

async def check_key_logic(id: int, session: dict):
    from telethon import errors
    
    # 1. Get the Key
    key_rows = await fetch_all("SELECT * FROM api_keys WHERE id = ?", (id,))
    if not key_rows:
        return {"status": "error", "error": "API Key not found"}
    
    key = key_rows[0]
    api_id = key["api_id"]
    api_hash = key["api_hash"]
    
    client = None
    status = "valid"
    error_msg = None
    
    try:
        client = await build_client_from_session(session, use_rotating_api=False, override_api=(api_id, api_hash))
        await client.connect()
        
        if not await client.is_user_authorized():
            status = "unknown" 
            error_msg = "账号与此Key不兼容或Key异常"
        else:
             me = await client.get_me()
             status = "valid"
             error_msg = "正常"

    except (errors.ApiIdInvalidError, errors.ApiIdPublishedFloodError) as e:
        status = "invalid"
        error_msg = f"无效: {str(e)}"
    except Exception as e:
        status = "error"
        error_msg = f"错误: {str(e)}"
    finally:
        if client:
            await client.disconnect()
            
    # Update DB
    new_desc = f"[{status.upper()}] {error_msg}"
    await execute("UPDATE api_keys SET description = ? WHERE id = ?", (new_desc, id))
    
    return {"status": status, "error": error_msg}
