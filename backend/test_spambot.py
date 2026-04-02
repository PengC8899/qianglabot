import asyncio
from database import fetch_one, init_db
from worker import build_client_from_session

async def main():
    await init_db()
    s = await fetch_one("SELECT * FROM sessions LIMIT 1")
    if not s:
        print("No sessions found")
        return
    print(f"Testing session {s['id']} ({s['phone']})")
    client = await build_client_from_session(s, use_rotating_api=False)
    try:
        await client.connect()
        async with client.conversation('@spambot', timeout=5) as conv:
            await conv.send_message('/start')
            resp = await conv.get_response()
            print("SpamBot replied:", resp.text)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.disconnect()

asyncio.run(main())
