import asyncio
from telethon import TelegramClient
from telethon.tl.functions.account import UpdateProfileRequest
from backend.database import fetch_one, SESSION_DIR
import os

async def main():
    session_row = await fetch_one("SELECT * FROM sessions WHERE status = 'active' LIMIT 1")
    if not session_row:
        print("No active session")
        return
    client = TelegramClient(
        os.path.join(SESSION_DIR, session_row["session_file"]),
        session_row["api_id"],
        session_row["api_hash"]
    )
    await client.connect()
    try:
        await client(UpdateProfileRequest(first_name="TestName", about=None))
        print("Success!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.disconnect()

asyncio.run(main())
