import asyncio
import time
import random
import traceback
from typing import List, Dict
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from telethon import TelegramClient, errors
from telethon.tl.functions.channels import JoinChannelRequest, InviteToChannelRequest, GetParticipantRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import ChannelParticipantAdmin, ChannelParticipantCreator

from database import fetch_all, execute
from worker import build_client_from_session, extract_group_target

router = APIRouter(prefix="/invite_v2", tags=["invite_v2"])

# --- 1. 账号入群模块 ---
async def join_group(client: TelegramClient, invite_link: str) -> bool:
    try:
        parsed = extract_group_target(invite_link)
        if parsed["is_invite"]:
            try:
                await client(ImportChatInviteRequest(parsed["target"]))
                return True
            except errors.UserAlreadyParticipantError:
                return True
        else:
            try:
                await client(JoinChannelRequest(parsed["target"]))
                return True
            except errors.UserAlreadyParticipantError:
                return True
    except Exception as e:
        print(f"Join group error: {e}")
        return False

# --- 2. 账号状态检测模块 ---
async def check_account_status(client: TelegramClient, group_link: str) -> dict:
    status_dict = {
        "is_online": False,
        "is_in_group": False,
        "is_admin": False,
        "can_invite": False,
        "error": None
    }
    try:
        if not await client.is_user_authorized():
            status_dict["error"] = "Unauthorized"
            return status_dict
        status_dict["is_online"] = True
        
        parsed = extract_group_target(group_link)
        target = parsed["target"] if not parsed["is_invite"] else group_link
        
        try:
            entity = await client.get_entity(target)
            me = await client.get_me()
            participant = await client(GetParticipantRequest(channel=entity, participant=me.id))
            part = participant.participant
            
            status_dict["is_in_group"] = True
            if isinstance(part, ChannelParticipantCreator):
                status_dict["is_admin"] = True
                status_dict["can_invite"] = True
            elif isinstance(part, ChannelParticipantAdmin):
                status_dict["is_admin"] = True
                rights = getattr(part, "admin_rights", None)
                status_dict["can_invite"] = bool(rights and getattr(rights, "invite_users", False))
        except errors.UserNotParticipantError:
            pass
        except Exception as e:
            status_dict["error"] = str(e)
            
    except Exception as e:
        status_dict["error"] = str(e)
        
    return status_dict

# In-memory store for account statuses
account_states: Dict[int, dict] = {}

class RefreshRequest(BaseModel):
    group_link: str

# --- 3. UI任务列表 (FastAPI) ---
@router.get("/accounts")
async def get_accounts():
    sessions = await fetch_all("SELECT * FROM sessions ORDER BY id ASC")
    results = []
    for s in sessions:
        sid = s["id"]
        state = account_states.get(sid, {})
        results.append({
            "session_id": sid,
            "phone": s["phone"],
            "status": s["status"],
            "is_in_group": state.get("is_in_group", False),
            "is_admin": state.get("is_admin", False),
            "can_invite": state.get("can_invite", False),
            "success_count": state.get("success_count", 0),
            "fail_count": state.get("fail_count", 0),
            "error": state.get("error")
        })
    return {"items": results}

@router.post("/accounts/join_all")
async def join_all_accounts(req: RefreshRequest, background_tasks: BackgroundTasks):
    sessions = await fetch_all("SELECT * FROM sessions WHERE status = 'active'")
    
    async def _run_join():
        async def _join(s):
            sid = s["id"]
            client = None
            try:
                client = await build_client_from_session(s)
                await client.connect()
                if await client.is_user_authorized():
                    success = await join_group(client, req.group_link)
                    if sid not in account_states:
                        account_states[sid] = {"success_count": 0, "fail_count": 0}
                    account_states[sid]["is_in_group"] = success
            except Exception as e:
                pass
            finally:
                if client and client.is_connected():
                    await client.disconnect()

        chunk_size = 5
        for i in range(0, len(sessions), chunk_size):
            chunk = sessions[i:i+chunk_size]
            tasks = [_join(s) for s in chunk]
            await asyncio.gather(*tasks)
            await asyncio.sleep(1)

    background_tasks.add_task(_run_join)
    return {"message": "已在后台开始一键进群"}

@router.post("/accounts/leave_all")
async def leave_all_accounts(req: RefreshRequest, background_tasks: BackgroundTasks):
    sessions = await fetch_all("SELECT * FROM sessions WHERE status = 'active'")
    
    async def _run_leave():
        async def _leave(s):
            sid = s["id"]
            # Only process accounts that are known to be in the group
            if sid in account_states and not account_states[sid].get("is_in_group", False):
                return
                
            client = None
            try:
                client = await build_client_from_session(s)
                await client.connect()
                if await client.is_user_authorized():
                    try:
                        parsed = extract_group_target(req.group_link)
                        target = parsed["target"] if not parsed["is_invite"] else req.group_link
                        entity = await client.get_entity(target)
                        await client(LeaveChannelRequest(entity))
                        
                        if sid in account_states:
                            account_states[sid]["is_in_group"] = False
                            account_states[sid]["is_admin"] = False
                            account_states[sid]["can_invite"] = False
                    except Exception as e:
                        pass # Ignore if not in group or other errors
            except Exception as e:
                pass
            finally:
                if client and client.is_connected():
                    await client.disconnect()

        chunk_size = 5
        for i in range(0, len(sessions), chunk_size):
            chunk = sessions[i:i+chunk_size]
            tasks = [_leave(s) for s in chunk]
            await asyncio.gather(*tasks)
            await asyncio.sleep(1)

    background_tasks.add_task(_run_leave)
    return {"message": "已在后台开始一键退群"}

@router.post("/accounts/refresh")
async def refresh_accounts(req: RefreshRequest, background_tasks: BackgroundTasks):
    sessions = await fetch_all("SELECT * FROM sessions WHERE status = 'active'")
    
    async def _run_refresh():
        async def _check(s):
            sid = s["id"]
            client = None
            try:
                client = await build_client_from_session(s)
                await client.connect()
                status = await check_account_status(client, req.group_link)
                
                if sid not in account_states:
                    account_states[sid] = {"success_count": 0, "fail_count": 0}
                account_states[sid].update(status)
            except Exception as e:
                if sid not in account_states:
                    account_states[sid] = {"success_count": 0, "fail_count": 0}
                account_states[sid]["error"] = str(e)
            finally:
                if client and client.is_connected():
                    await client.disconnect()

        chunk_size = 5
        for i in range(0, len(sessions), chunk_size):
            chunk = sessions[i:i+chunk_size]
            tasks = [_check(s) for s in chunk]
            await asyncio.gather(*tasks)
            await asyncio.sleep(1)

    background_tasks.add_task(_run_refresh)
    return {"message": "已在后台开始检测账号状态"}

# --- 5. 任务执行器 (队列化) ---
invite_queue = asyncio.Queue()
invite_logs = []
invite_stats = {"success": 0, "fail": 0}

async def process_invite_task(task):
    target_user = task["username"]
    group_link = task["group_link"]
    
    # Filter available accounts
    available_sids = [
        sid for sid, state in account_states.items()
        if state.get("is_admin") and state.get("can_invite")
    ]
    
    if not available_sids:
        invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 邀请 {target_user} -> 失败: 没有可用的管理员账号。")
        invite_stats["fail"] += 1
        return
        
    # Pick random admin account
    sid = random.choice(available_sids)
    
    try:
        session_row = await fetch_all("SELECT * FROM sessions WHERE id = ?", (sid,))
        if not session_row:
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 邀请 {target_user} -> 失败: 找不到账号 {sid}。")
            invite_stats["fail"] += 1
            return
            
        client = None
        try:
            client = await build_client_from_session(session_row[0])
            await client.connect()
            
            parsed = extract_group_target(group_link)
            group_entity = await client.get_entity(parsed["target"] if not parsed["is_invite"] else group_link)
            
            # Resolve target
            try:
                target_entity = await client.get_entity(target_user)
            except ValueError:
                # Target not found
                if sid in account_states:
                    account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
                invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (用户不存在/找不到)")
                invite_stats["fail"] += 1
                if client.is_connected():
                    await client.disconnect()
                return
            
            await client(InviteToChannelRequest(channel=group_entity, users=[target_entity]))
            
            if sid in account_states:
                account_states[sid]["success_count"] = account_states[sid].get("success_count", 0) + 1
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 成功")
            invite_stats["success"] += 1
            
        except errors.FloodWaitError as e:
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (FloodWait: {e.seconds}s)")
            invite_stats["fail"] += 1
            await asyncio.sleep(e.seconds)
        except errors.UserPrivacyRestrictedError:
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (隐私限制)")
            invite_stats["fail"] += 1
        except errors.ChatAdminRequiredError:
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
                account_states[sid]["is_admin"] = False # mark as not admin
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (非管理员)")
            invite_stats["fail"] += 1
        except errors.UserAlreadyParticipantError:
            if sid in account_states:
                account_states[sid]["success_count"] = account_states[sid].get("success_count", 0) + 1
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 成功 (已在群内)")
            invite_stats["success"] += 1
        except errors.PeerFloodError:
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (严重风控 PeerFlood)")
            invite_stats["fail"] += 1
        except Exception as e:
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 ({str(e)})")
            invite_stats["fail"] += 1
        finally:
            if client and client.is_connected():
                await client.disconnect()
            
        # Delay to avoid flood
        await asyncio.sleep(random.uniform(10, 30))
    except Exception as e:
        invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 邀请 {target_user} -> 失败: 系统错误 ({str(e)})")
        invite_stats["fail"] += 1

async def invite_worker():
    while True:
        try:
            task = await invite_queue.get()
            try:
                # Run the task concurrently without blocking the queue
                asyncio.create_task(process_invite_task(task))
            except Exception as e:
                print(f"Error starting task: {e}")
                traceback.print_exc()
            finally:
                invite_queue.task_done()
            
            # Small delay to prevent queue exhaustion instantly
            await asyncio.sleep(0.5)
        except Exception as e:
            print(f"Fatal error in invite_worker loop: {e}")
            traceback.print_exc()
            await asyncio.sleep(5)


@router.on_event("startup")
async def startup_event():
    asyncio.create_task(invite_worker())

# --- 4. 邀请任务核心逻辑 ---
class InviteRequest(BaseModel):
    username: str
    group_link: str

@router.post("/invite")
async def add_invite_task(req: InviteRequest):
    await invite_queue.put({
        "username": req.username,
        "group_link": req.group_link
    })
    return {"message": "Task queued"}

@router.post("/stop_all")
async def stop_all_invites():
    # Empty the queue
    count = 0
    while not invite_queue.empty():
        try:
            invite_queue.get_nowait()
            invite_queue.task_done()
            count += 1
        except asyncio.QueueEmpty:
            break
    add_log(f"已手动停止任务，清空了 {count} 个待邀请目标。")
    return {"message": f"已停止，清空了 {count} 个任务"}

@router.get("/logs")
async def get_invite_logs():
    return {
        "logs": invite_logs[-100:],
        "stats": invite_stats
    }
