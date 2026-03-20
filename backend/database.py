import aiosqlite
import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "data.db")
SESSION_DIR = os.path.join(BASE_DIR, "sessions")

def get_db():
    return aiosqlite.connect(DB_PATH)

async def init_db():
    os.makedirs(SESSION_DIR, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        # Health score for Session
        # Adding 'health_score' and 'proxy_id' to sessions table requires migration script or manual SQL
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT,
                api_id INTEGER,
                api_hash TEXT,
                session_file TEXT,
                status TEXT,
                last_used TEXT,
                flood_wait INTEGER,
                health_score INTEGER DEFAULT 100,
                proxy_id INTEGER,
                nickname TEXT,
                session_string TEXT,
                is_manager INTEGER DEFAULT 0
            )
            """
        )
        # Migration for existing table
        try:
            await db.execute("ALTER TABLE sessions ADD COLUMN nickname TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE sessions ADD COLUMN session_string TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE sessions ADD COLUMN is_manager INTEGER DEFAULT 0")
        except:
            pass
            
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT,
                targets TEXT,
                delay_seconds INTEGER,
                random_delay INTEGER,
                max_per_account INTEGER,
                status TEXT,
                total_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                fail_count INTEGER DEFAULT 0,
                task_type TEXT DEFAULT 'dm',
                group_link TEXT,
                created_at TEXT,
                allowed_session_ids TEXT
            )
            """
        )
        # Check and migrate columns if needed (simplified migration for existing DB)
        try:
            await db.execute("ALTER TABLE tasks ADD COLUMN total_count INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE tasks ADD COLUMN success_count INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE tasks ADD COLUMN fail_count INTEGER DEFAULT 0")
        except:
            pass
        try:
            await db.execute("ALTER TABLE tasks ADD COLUMN task_type TEXT DEFAULT 'dm'")
        except:
            pass
        try:
            await db.execute("ALTER TABLE tasks ADD COLUMN group_link TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE tasks ADD COLUMN allowed_session_ids TEXT")
        except:
            pass
            
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER,
                session_id INTEGER,
                target TEXT,
                status TEXT,
                error TEXT,
                time TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS login_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT,
                api_id INTEGER,
                api_hash TEXT,
                session_file TEXT,
                phone_code_hash TEXT,
                created_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                reason TEXT,
                created_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS proxies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                status TEXT,
                fail_count INTEGER,
                last_used TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS task_targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER,
                target TEXT,
                status TEXT DEFAULT 'pending',
                worker_session_id INTEGER,
                error TEXT,
                executed_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_id INTEGER,
                api_hash TEXT,
                description TEXT,
                created_at TEXT,
                last_used_at TEXT
            )
            """
        )
        try:
            await db.execute("ALTER TABLE api_keys ADD COLUMN last_used_at TEXT")
        except:
            pass
            
        # Add worker_id to sessions to track which task is using it
        try:
            await db.execute("ALTER TABLE sessions ADD COLUMN current_task_id INTEGER")
        except:
            pass

        await db.commit()


async def execute(query: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH, timeout=20.0) as db:
        await db.execute(query, params)
        await db.commit()


async def execute_returning_id(query: str, params: tuple = ()) -> int:
    async with aiosqlite.connect(DB_PATH, timeout=20.0) as db:
        cursor = await db.execute(query, params)
        await db.commit()
        return cursor.lastrowid


async def fetch_all(query: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH, timeout=20.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def fetch_one(query: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH, timeout=20.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


def now_iso():
    return datetime.utcnow().isoformat()


def serialize_targets(targets):
    return json.dumps(targets, ensure_ascii=False)


def deserialize_targets(raw):
    return json.loads(raw) if raw else []
