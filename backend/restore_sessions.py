import asyncio
import os
import sqlite3
from database import fetch_all, execute, init_db, SESSION_DIR

async def restore():
    await init_db()
    existing = await fetch_all("SELECT session_file FROM sessions")
    existing_files = {row["session_file"] for row in existing}
    
    files = [f for f in os.listdir(SESSION_DIR) if f.endswith(".session")]
    restored_count = 0
    for f in files:
        if f not in existing_files:
            phone = f.replace(".session", "")
            # Using default API ID and Hash for restored sessions
            api_id = 35019294
            api_hash = "9e2d91fe6876d834bae4707b0875e2d7"
            await execute(
                "INSERT INTO sessions (phone, api_id, api_hash, session_file, status) VALUES (?, ?, ?, ?, ?)",
                (phone, api_id, api_hash, f, "active")
            )
            restored_count += 1
            
    print(f"Restored {restored_count} sessions.")

if __name__ == "__main__":
    asyncio.run(restore())