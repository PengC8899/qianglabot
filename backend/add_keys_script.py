import sqlite3
import os
from datetime import datetime

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "data.db")

def add_keys():
    keys = [
        (34448762, "76cb0bdb5875504f5f63d2eb0093d437"),
        (33244477, "5b108653c040622220d92627a7e5edbf"),
        (39211440, "f8dd6f1f0f671649b605cb912c7a8e04"),
        (36902678, "1d25e4067aabb72cc21726d047a0ec61")
    ]
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    added = 0
    for api_id, api_hash in keys:
        # Check if exists
        cursor.execute("SELECT id FROM api_keys WHERE api_id = ?", (api_id,))
        if cursor.fetchone():
            print(f"Skipping {api_id}: already exists")
            continue
            
        cursor.execute(
            "INSERT INTO api_keys (api_id, api_hash, created_at) VALUES (?, ?, ?)",
            (api_id, api_hash, datetime.now().isoformat())
        )
        added += 1
        print(f"Added {api_id}")
        
    conn.commit()
    conn.close()
    print(f"Total added: {added}")

if __name__ == "__main__":
    add_keys()
