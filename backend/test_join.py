import asyncio
import os
from database import fetch_one, init_db, SESSION_DIR
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from utils import get_proxy_config

async def main():
    await init_db()
    s = await fetch_one("SELECT * FROM sessions WHERE status = 'active' ORDER BY id DESC LIMIT 1")
    if not s:
        print("No active sessions")
        return
    
    print(f"Testing session {s['id']} ({s['phone']})")
    proxy = await get_proxy_config()
    client = TelegramClient(
        StringSession(s["session_string"]) if s["session_string"] else os.path.join(SESSION_DIR, s["session_file"]),
        s["api_id"],
        s["api_hash"],
        proxy=proxy
    )
    try:
        await client.connect()
        me = await client.get_me()
        print(f"Logged in as {me.first_name}")
        
        print("Attempting to join @Durov...")
        await client(JoinChannelRequest('durov'))
        print("Joined successfully!")
    except Exception as e:
        print(f"Join failed: {type(e).__name__} - {e}")
    finally:
        await client.disconnect()

asyncio.run(main())
