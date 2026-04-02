import asyncio
import os
from database import fetch_all, init_db, SESSION_DIR
from telethon import TelegramClient
from utils import get_device_fingerprint

async def main():
    await init_db()
    sessions = await fetch_all("SELECT id, phone, status, session_file FROM sessions WHERE id = 2222")
    for s in sessions:
        print(f"Checking session {s['id']} ({s['phone']}) - Current Status: {s['status']}")
        try:
            fp = get_device_fingerprint()
            base_name = s["session_file"].replace(".session", "")
            session_path = os.path.join(SESSION_DIR, base_name)
            print(f"Using path: {session_path}")
            client = TelegramClient(session_path, 35019294, "9e2d91fe6876d834bae4707b0875e2d7", **fp)
            await client.connect()
            if not await client.is_user_authorized():
                print("Not authorized")
            else:
                me = await client.get_me()
                print(f"Logged in as {me.first_name}, restricted: {me.restricted}")
                
                # spambot check
                try:
                    async with client.conversation('@spambot', timeout=5) as conv:
                        await conv.send_message('/start')
                        resp = await conv.get_response()
                        print(f"Spambot says: {resp.text}")
                except Exception as e:
                    print(f"Spambot error: {e}")
            await client.disconnect()
        except Exception as e:
            import traceback
            traceback.print_exc()

asyncio.run(main())
