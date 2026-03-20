import asyncio


from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from database import fetch_all, fetch_one, get_db

router = APIRouter()

class LogHub:
    def __init__(self):
        self.connections = set()
        self.lock = asyncio.Lock()

    async def connect(self, websocket, task_id):
        await websocket.accept()
        async with self.lock:
            self.connections.add((websocket, task_id))

    async def disconnect(self, websocket):
        async with self.lock:
            self.connections = {
                (ws, task_id) for (ws, task_id) in self.connections if ws != websocket
            }

    async def broadcast(self, message, task_id=None):
        async with self.lock:
            targets = list(self.connections)
        for ws, subscribed_task in targets:
            if task_id is not None and subscribed_task is not None:
                if str(task_id) != str(subscribed_task):
                    continue
            try:
                await ws.send_json(message)
            except Exception:
                await self.disconnect(ws)


log_hub = LogHub()

@router.get("/stats")
async def get_log_stats(task_id: int | None = None):
    # If task_id not provided, try to find the latest task
    limit = 0
    target_task_id = task_id
    
    if target_task_id is None:
        latest_task = await fetch_one("SELECT id, max_per_account FROM tasks ORDER BY id DESC LIMIT 1")
        if latest_task:
            target_task_id = latest_task["id"]
            limit = latest_task["max_per_account"]
    else:
        task = await fetch_one("SELECT max_per_account FROM tasks WHERE id = ?", (target_task_id,))
        if task:
            limit = task["max_per_account"]
            
    if not target_task_id:
        return {"limit": 0, "stats": []}

    query = """
        SELECT 
            l.task_id,
            l.session_id, 
            s.phone,
            SUM(CASE WHEN l.status = 'success' THEN 1 ELSE 0 END) as success,
            SUM(CASE WHEN l.status = 'failed' OR l.status = 'flood_wait' THEN 1 ELSE 0 END) as failed
        FROM logs l
        LEFT JOIN sessions s ON l.session_id = s.id
        WHERE l.task_id = ? AND l.session_id IS NOT NULL
        GROUP BY l.session_id, s.phone, l.task_id
    """
    
    rows = await fetch_all(query, (target_task_id,))
    return {
        "limit": limit,
        "stats": [dict(row) for row in rows]
    }

@router.get("")
async def list_logs(task_id: int | None = None):
    if task_id is None:
        rows = await fetch_all("SELECT * FROM logs ORDER BY id DESC LIMIT 200")
    else:
        rows = await fetch_all(
            "SELECT * FROM logs WHERE task_id = ? ORDER BY id DESC LIMIT 200",
            (task_id,),
        )
    return {"items": [dict(row) for row in rows]}

@router.websocket("/ws")
async def logs_ws(websocket: WebSocket, task_id: int | None = None):
    await log_hub.connect(websocket, task_id)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await log_hub.disconnect(websocket)
