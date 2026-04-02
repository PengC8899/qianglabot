import asyncio
import os
from database import fetch_one, init_db, SESSION_DIR
from telethon import TelegramClient
from telethon.sessions import StringSession
from utils import get_proxy_config

async def main():
    await init_db()
    s = await fetch_one("SELECT * FROM sessions WHERE id = 2170")
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
        dialogs = await client.get_dialogs()
        print(f"Got {len(dialogs)} dialogs")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.disconnect()

asyncio.run(main())
