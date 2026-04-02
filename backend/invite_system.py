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
    except errors.UserAlreadyParticipantError:
        return True
    except Exception as e:
        error_msg = str(e).lower()
        print(f"Join group error: {e}")
        # 如果报错里明确包含由于已经存在而无法加入等情况，也可以根据实际错误字符串处理
        if "already" in error_msg or "participant" in error_msg:
             return True
        if "frozen_method_invalid" in error_msg:
             print(f"Account restricted from joining via this method (frozen)")
             # We return False here as it genuinely failed to join
             return False
        # 如果报错提示找不到该用户名（说明可能是一个私人链接被解析错或者群被封了等），我们依然先返回 False，防止误判
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
            "error": state.get("error"),
            "join_status": state.get("join_status", "")
        })
    return {"items": results}

@router.post("/accounts/join_all")
async def join_all_accounts(req: RefreshRequest, background_tasks: BackgroundTasks):
    sessions = await fetch_all("SELECT * FROM sessions WHERE status = 'active'")
    
    async def _run_join():
        async def _join(s):
            sid = s["id"]
            client = None
            lock = get_account_lock(sid)
            async with lock:
                try:
                    if sid not in account_states:
                        account_states[sid] = {"success_count": 0, "fail_count": 0}
                    account_states[sid]["join_status"] = "joining"

                    async def do_join():
                        nonlocal client
                        client = await build_client_from_session(s)
                        await client.connect()
                        if await client.is_user_authorized():
                            # 先检查是否已经在群里，如果是直接返回成功
                            status = await check_account_status(client, req.group_link)
                            if status.get("is_in_group"):
                                # 更新全局状态为在群里
                                account_states[sid]["is_in_group"] = True
                                return True
                            return await join_group(client, req.group_link)
                        return False

                    success = await asyncio.wait_for(do_join(), timeout=30.0)
                    account_states[sid]["is_in_group"] = success
                    account_states[sid]["join_status"] = "success" if success else "failed"
                    if not success:
                        account_states[sid]["error"] = "Join group failed or unauthorized"
                        
                except asyncio.TimeoutError:
                    if sid in account_states:
                        account_states[sid]["join_status"] = "failed"
                        account_states[sid]["error"] = "Join timeout"
                except Exception as e:
                    if sid in account_states:
                        account_states[sid]["join_status"] = "failed"
                        account_states[sid]["error"] = str(e)
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
            lock = get_account_lock(sid)
            async with lock:
                try:
                    async def do_leave():
                        nonlocal client
                        client = await build_client_from_session(s)
                        await client.connect()
                        if await client.is_user_authorized():
                            parsed = extract_group_target(req.group_link)
                            target = parsed["target"] if not parsed["is_invite"] else req.group_link
                            entity = await client.get_entity(target)
                            await client(LeaveChannelRequest(entity))
                    
                    await asyncio.wait_for(do_leave(), timeout=30.0)
                    if sid in account_states:
                        account_states[sid]["is_in_group"] = False
                        account_states[sid]["is_admin"] = False
                        account_states[sid]["can_invite"] = False
                except asyncio.TimeoutError:
                    pass
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
            lock = get_account_lock(sid)
            async with lock:
                try:
                    async def do_check():
                        nonlocal client
                        client = await build_client_from_session(s)
                        await client.connect()
                        return await check_account_status(client, req.group_link)
                    
                    status = await asyncio.wait_for(do_check(), timeout=30.0)
                    if sid not in account_states:
                        account_states[sid] = {"success_count": 0, "fail_count": 0}
                    account_states[sid].update(status)
                except asyncio.TimeoutError:
                    if sid not in account_states:
                        account_states[sid] = {"success_count": 0, "fail_count": 0}
                    account_states[sid]["error"] = "连接超时 (Timeout)"
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
            try:
                await asyncio.gather(*tasks)
            except Exception as e:
                print(f"Error in gather: {e}")
                traceback.print_exc()
            await asyncio.sleep(1)
        print("Refresh task completed.")

    print(f"Starting refresh for {len(sessions)} sessions...")
    background_tasks.add_task(_run_refresh)
    return {"message": "已在后台开始检测账号状态"}

# --- 5. 任务执行器 (队列化) ---
invite_queue = asyncio.Queue()
invite_logs = []
invite_stats = {"success": 0, "fail": 0}
# Global flag to control stopping running tasks
stop_flag = {"is_stopped": False}

async def stop_invite_queue(reason: str):
    stop_flag["is_stopped"] = True
    count = 0
    while not invite_queue.empty():
        try:
            invite_queue.get_nowait()
            invite_queue.task_done()
            count += 1
        except asyncio.QueueEmpty:
            break
    invite_logs.append(f"[{time.strftime('%H:%M:%S')}] {reason} 清空了 {count} 个待邀请目标。")
    return count

async def process_invite_task(task):
    if stop_flag["is_stopped"]:
        return
        
    target_user = task["username"]
    group_link = task["group_link"]
    retry_count = task.get("retry_count", 0)
    
    # Filter available accounts not in cooldown
    current_time = time.time()
    available_sids = [
        sid for sid, state in account_states.items()
        if state.get("is_admin") 
        and state.get("can_invite")
        and current_time > state.get("cooldown_until", 0)
    ]
    
    if not available_sids:
        has_admins = any(state.get("is_admin") and state.get("can_invite") for state in account_states.values())
        if has_admins:
            cooldown_seconds = []
            for state in account_states.values():
                if state.get("is_admin") and state.get("can_invite"):
                    remaining = int((state.get("cooldown_until", 0) or 0) - current_time)
                    if remaining > 0:
                        cooldown_seconds.append(remaining)
            wait_seconds = min(cooldown_seconds) if cooldown_seconds else 0
            await stop_invite_queue(f"邀请 {target_user} -> 自动停止: 所有管理号都在冷却中（最短还需 {wait_seconds}s）。")
        else:
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 邀请 {target_user} -> 失败: 彻底没有可用的管理员账号。")
            invite_stats["fail"] += 1
        return
        
    sid = min(
        available_sids,
        key=lambda x: (
            account_states.get(x, {}).get("last_invite_at", 0),
            account_states.get(x, {}).get("fail_count", 0),
            random.random()
        )
    )
    
    lock = get_account_lock(sid)
    async with lock:
        if stop_flag["is_stopped"]:
            return

        client = None
        try:
            async def _do_invite():
                nonlocal client
                session_row = await fetch_all("SELECT * FROM sessions WHERE id = ?", (sid,))
                if not session_row:
                    return False, "找不到账号", None
                    
                client = await build_client_from_session(session_row[0])
                await client.connect()
                
                parsed = extract_group_target(group_link)
                group_entity = await client.get_entity(parsed["target"] if not parsed["is_invite"] else group_link)
                
                try:
                    target_entity = await client.get_entity(target_user)
                except ValueError:
                    return False, "用户不存在/找不到", None
                
                try:
                    await client(InviteToChannelRequest(channel=group_entity, users=[target_entity]))
                    await asyncio.sleep(random.uniform(2.5, 4.5))
                    try:
                        participant = await client(GetParticipantRequest(channel=group_entity, participant=target_entity))
                        if not participant:
                            return True, "邀请请求已发送，等待目标端状态同步", None
                    except errors.UserNotParticipantError:
                        return True, "邀请请求已发送，目标未即时可见", None
                    except Exception:
                        pass
                except Exception as inner_e:
                    error_msg = str(inner_e).lower()
                    if "already" in error_msg or "participant" in error_msg:
                        return False, "对方已经在群里了", None
                    if "privacy" in error_msg:
                        return False, "对方隐私限制", None
                    if "too_much" in error_msg or "flood" in error_msg or "too many requests" in error_msg or "too_many_requests" in error_msg:
                        raise inner_e
                    if "chat_member_add_failed" in error_msg or "user_not_mutual_contact" in error_msg:
                        return False, "无法强拉：对方隐私或账号风控限制", None
                    return False, f"邀请异常: {str(inner_e)}", None
                    
                return True, "成功", None

            # Wrap network ops with timeout to prevent hangs
            success, msg, _ = await asyncio.wait_for(_do_invite(), timeout=60.0)
            
            if success:
                if sid in account_states:
                    account_states[sid]["success_count"] = account_states[sid].get("success_count", 0) + 1
                    account_states[sid]["last_invite_at"] = time.time()
                if "邀请请求已发送" in (msg or ""):
                    invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 成功[已发送邀请请求] ({msg})")
                else:
                    invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 成功[已确认进群]")
                invite_stats["success"] += 1
            else:
                if sid in account_states:
                    account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
                    account_states[sid]["last_invite_at"] = time.time()
                invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 ({msg})")
                invite_stats["fail"] += 1
                
        except asyncio.TimeoutError:
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
                account_states[sid]["cooldown_until"] = time.time() + 60
                account_states[sid]["last_invite_at"] = time.time()
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (网络/代理连接超时卡死)")
            if retry_count < 2 and not stop_flag["is_stopped"]:
                await invite_queue.put({**task, "retry_count": retry_count + 1})
            else:
                invite_stats["fail"] += 1
                
        except errors.FloodWaitError as e:
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
                account_states[sid]["cooldown_until"] = time.time() + e.seconds
                account_states[sid]["last_invite_at"] = time.time()
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (FloodWait: {e.seconds}s)")
            if retry_count < 3 and not stop_flag["is_stopped"]:
                await invite_queue.put({**task, "retry_count": retry_count + 1})
            else:
                invite_stats["fail"] += 1
                
        except errors.UserPrivacyRestrictedError:
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
                account_states[sid]["last_invite_at"] = time.time()
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (对方隐私限制)")
            invite_stats["fail"] += 1
            
        except errors.ChatAdminRequiredError:
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
                account_states[sid]["is_admin"] = False
                account_states[sid]["last_invite_at"] = time.time()
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (当前号失去拉人权限)")
            if retry_count < 3 and not stop_flag["is_stopped"]:
                await invite_queue.put({**task, "retry_count": retry_count + 1})
            else:
                invite_stats["fail"] += 1
                
        except errors.UserAlreadyParticipantError:
            if sid in account_states:
                account_states[sid]["success_count"] = account_states[sid].get("success_count", 0) + 1
                account_states[sid]["last_invite_at"] = time.time()
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 成功[已确认进群] (已在群内)")
            invite_stats["success"] += 1
            
        except errors.PeerFloodError:
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
                # Increase cooldown significantly for PeerFlood (e.g. 24 hours)
                account_states[sid]["cooldown_until"] = time.time() + 86400
                account_states[sid]["last_invite_at"] = time.time()
            invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (严重风控 PeerFlood)")
            if retry_count < 3 and not stop_flag["is_stopped"]:
                await invite_queue.put({**task, "retry_count": retry_count + 1})
            else:
                invite_stats["fail"] += 1
                
        except Exception as e:
            error_str = str(e)
            error_str_lower = error_str.lower()
            if sid in account_states:
                account_states[sid]["fail_count"] = account_states[sid].get("fail_count", 0) + 1
                account_states[sid]["last_invite_at"] = time.time()
                
            if "chat_member_add_failed" in error_str_lower or "user_not_mutual_contact" in error_str_lower:
                invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (对方隐私设置/账号风控，无法强拉)")
                invite_stats["fail"] += 1
            elif "too many channels" in error_str_lower:
                invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 (对方加群已达上限)")
                invite_stats["fail"] += 1
            elif "too many requests" in error_str_lower or "too_many_requests" in error_str_lower or "flood" in error_str_lower:
                if sid in account_states:
                    account_states[sid]["cooldown_until"] = time.time() + 600
                invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 暂停 (请求过频，账号冷却10分钟)")
                if retry_count < 2 and not stop_flag["is_stopped"]:
                    await invite_queue.put({**task, "retry_count": retry_count + 1})
                else:
                    invite_stats["fail"] += 1
            else:
                invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 账号 {sid} -> 邀请 {target_user} -> 失败 ({error_str})")
                if retry_count < 3 and not stop_flag["is_stopped"]:
                    await invite_queue.put({**task, "retry_count": retry_count + 1})
                else:
                    invite_stats["fail"] += 1
            
        finally:
            if client and client.is_connected():
                await client.disconnect()
            
        # Normal delay to prevent spamming from the SAME account too fast
        # Note: We no longer sleep for `e.seconds` inside the lock.
        # This keeps the semaphore and the account lock free for others.
        if not stop_flag["is_stopped"]:
            await asyncio.sleep(random.uniform(12, 28))

invite_semaphore = asyncio.Semaphore(2)
account_locks = {}

def get_account_lock(sid):
    if sid not in account_locks:
        account_locks[sid] = asyncio.Lock()
    return account_locks[sid]

async def process_invite_with_sem(task):
    async with invite_semaphore:
        await process_invite_task(task)

async def invite_worker():
    while True:
        try:
            task = await invite_queue.get()
            try:
                await process_invite_with_sem(task)
            except Exception as e:
                print(f"Error starting task: {e}")
                traceback.print_exc()
            finally:
                invite_queue.task_done()
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
    # Reset stop flag when starting new tasks
    stop_flag["is_stopped"] = False
    
    await invite_queue.put({
        "username": req.username,
        "group_link": req.group_link
    })
    return {"message": "Task queued"}

@router.post("/stop_all")
async def stop_all_invites():
    count = await stop_invite_queue("已手动停止任务，正在执行的任务也已终止。")
    return {"message": f"已停止，清空了 {count} 个任务"}

@router.get("/logs")
async def get_invite_logs():
    return {
        "logs": invite_logs,
        "stats": invite_stats
    }

@router.post("/accounts/clear_cooldown")
async def clear_invite_cooldowns():
    cleared = 0
    now_ts = time.time()
    for state in account_states.values():
        cooldown_until = float(state.get("cooldown_until", 0) or 0)
        if cooldown_until > now_ts:
            cleared += 1
        state["cooldown_until"] = 0
    invite_logs.append(f"[{time.strftime('%H:%M:%S')}] 已手动清除 {cleared} 个账号冷却状态。")
    return {"message": f"已清除 {cleared} 个账号冷却状态", "cleared": cleared}

@router.post("/clear_logs")
async def clear_invite_logs():
    invite_logs.clear()
    invite_stats["success"] = 0
    invite_stats["fail"] = 0
    return {"message": "Logs and stats cleared"}
