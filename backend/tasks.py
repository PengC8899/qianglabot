import asyncio
import json
from fastapi import APIRouter
from pydantic import BaseModel, Field
from database import execute_returning_id, fetch_all, now_iso, serialize_targets, get_db, execute
from worker import run_task, join_group_for_all_active_sessions, promote_all_sessions_to_admins, check_all_sessions_in_group

router = APIRouter(prefix="/tasks", tags=["tasks"])


class TaskCreateRequest(BaseModel):
    message: str
    targets: list[str] = Field(default_factory=list)
    delay_seconds: int = 20
    random_delay: bool = False
    max_per_account: int = 40


class InviteTaskCreateRequest(BaseModel):
    group_link: str
    targets: list[str] = Field(default_factory=list)
    delay_seconds: int = 20
    random_delay: bool = False
    max_per_account: int = 40


async def create_invite_task_record(
    group_link: str,
    targets: list[str],
    delay_seconds: int,
    random_delay: bool,
    max_per_account: int,
    invite_session_ids: list[int] | None = None,
):
    scoped_ids = [int(x) for x in (invite_session_ids or []) if str(x).isdigit()]
    task_id = await execute_returning_id(
        """
        INSERT INTO tasks (
            message, targets, delay_seconds, random_delay, max_per_account,
            status, total_count, success_count, fail_count, task_type, group_link, created_at, allowed_session_ids
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "invite_to_group",
            serialize_targets(targets),
            delay_seconds,
            1 if random_delay else 0,
            max_per_account,
            "queued",
            len(targets),
            0,
            0,
            "invite",
            group_link,
            now_iso(),
            json.dumps(scoped_ids, ensure_ascii=False) if scoped_ids else None,
        ),
    )
    asyncio.create_task(run_task(task_id))
    async with get_db() as db:
        for target in targets:
            await db.execute(
                "INSERT INTO task_targets (task_id, target, status) VALUES (?, ?, ?)",
                (task_id, target, "pending")
            )
        await db.commit()
    return {"task_id": task_id, "total_targets": len(targets), "invite_session_ids": scoped_ids}


async def get_active_manager_session_ids() -> list[int]:
    rows = await fetch_all(
        "SELECT id FROM sessions WHERE status = 'active' AND IFNULL(is_manager, 0) = 1 ORDER BY id ASC"
    )
    return [int(row["id"]) for row in rows if str(row.get("id", "")).isdigit()]


async def get_active_non_manager_session_ids() -> list[int]:
    rows = await fetch_all(
        "SELECT id FROM sessions WHERE status = 'active' AND IFNULL(is_manager, 0) = 0 ORDER BY id ASC"
    )
    return [int(row["id"]) for row in rows if str(row.get("id", "")).isdigit()]


async def get_active_session_ids() -> list[int]:
    rows = await fetch_all(
        "SELECT id FROM sessions WHERE status = 'active' ORDER BY id ASC"
    )
    return [int(row["id"]) for row in rows if str(row.get("id", "")).isdigit()]


@router.post("/create")
async def create_task(payload: TaskCreateRequest):
    # De-duplicate targets
    unique_targets = list(dict.fromkeys(payload.targets))
    
    task_id = await execute_returning_id(
        """
        INSERT INTO tasks (
            message, targets, delay_seconds, random_delay, max_per_account, 
            status, total_count, success_count, fail_count, task_type, group_link, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.message,
            serialize_targets(unique_targets),
            payload.delay_seconds,
            1 if payload.random_delay else 0,
            payload.max_per_account,
            "queued",
            len(unique_targets),
            0,
            0,
            "dm",
            None,
            now_iso(),
        ),
    )
    asyncio.create_task(run_task(task_id))
    
    # Bulk insert into task_targets
    async with get_db() as db:
        for target in unique_targets:
            await db.execute(
                "INSERT INTO task_targets (task_id, target, status) VALUES (?, ?, ?)",
                (task_id, target, "pending")
            )
        await db.commit()

    return {"task_id": task_id, "total_targets": len(unique_targets)}


@router.post("/invite/create")
async def create_invite_task(payload: InviteTaskCreateRequest):
    unique_targets = list(dict.fromkeys(payload.targets))
    group_link = payload.group_link.strip()
    if not group_link:
        return {"status": "error", "message": "Group link is required"}
    if not unique_targets:
        return {"status": "error", "message": "No targets provided"}
    return await create_invite_task_record(
        group_link=group_link,
        targets=unique_targets,
        delay_seconds=payload.delay_seconds,
        random_delay=payload.random_delay,
        max_per_account=payload.max_per_account,
    )


class CheckAccountsRequest(BaseModel):
    group_link: str

@router.post("/invite/check_accounts")
async def check_accounts(payload: CheckAccountsRequest):
    group_link = payload.group_link.strip()
    if not group_link:
        return {"status": "error", "message": "Group link is required"}
    
    return await check_all_sessions_in_group(group_link)

class InviteJoinRequest(BaseModel):
    group_link: str
    session_ids: list[int] = Field(default_factory=list)
    delay_seconds: int = 2
    random_delay: bool = True


@router.post("/invite/join_all")
async def join_group_all_sessions(payload: InviteJoinRequest):
    group_link = payload.group_link.strip()
    if not group_link:
        return {"status": "error", "message": "Group link is required"}
    requested_ids = [int(x) for x in (payload.session_ids or []) if str(x).isdigit()]
    join_ids = requested_ids if requested_ids else await get_active_non_manager_session_ids()
    if not join_ids:
        return {"status": "error", "message": "没有可执行加群的普通账号"}
    return await join_group_for_all_active_sessions(
        group_link,
        session_ids=join_ids,
        delay_seconds=payload.delay_seconds,
        random_delay=payload.random_delay,
    )


class PromoteAdminsRequest(BaseModel):
    group_link: str
    session_ids: list[int] = Field(default_factory=list)
    rank: str = "Admin"
    delay_seconds: int = 1
    random_delay: bool = True
    grant_add_admins: bool = True
    bootstrap_admin_count: int = 5
    manager_session_ids: list[int] = Field(default_factory=list)


@router.post("/invite/promote_admins")
async def promote_admins(payload: PromoteAdminsRequest):
    group_link = payload.group_link.strip()
    if not group_link:
        return {"status": "error", "message": "Group link is required"}
    manager_ids = [int(x) for x in (payload.manager_session_ids or []) if str(x).isdigit()]
    if not manager_ids:
        manager_ids = await get_active_manager_session_ids()
    if not manager_ids:
        return {"status": "error", "message": "请先在账号管理页完成管理号登录"}
    requested_ids = [int(x) for x in (payload.session_ids or []) if str(x).isdigit()]
    default_targets = await get_active_non_manager_session_ids()
    promote_ids = requested_ids if requested_ids else default_targets
    if not promote_ids:
        promote_ids = await get_active_session_ids()
    promote_ids = [sid for sid in promote_ids if sid not in manager_ids]
    if not promote_ids:
        return {"status": "error", "message": "没有可执行设管的协议号"}
    result = await promote_all_sessions_to_admins(
        group_link,
        session_ids=promote_ids,
        promoter_session_ids=manager_ids,
        rank=(payload.rank or "Admin").strip() or "Admin",
        delay_seconds=payload.delay_seconds,
        random_delay=payload.random_delay,
        grant_add_admins=payload.grant_add_admins,
        bootstrap_admin_count=payload.bootstrap_admin_count,
    )
    result["manager_session_ids"] = result.get("promoter_session_ids") or manager_ids
    result["target_session_ids"] = promote_ids
    if int(result.get("promoters") or 0) == 0:
        reason_stats = result.get("reason_stats") or {}
        if int(reason_stats.get("UNKNOWN_ERROR") or 0) > 0:
            return {
                "status": "error",
                "message": "群链接解析失败，请改用有效的 @群用户名 或 https://t.me/+邀请链接",
                "promote": result,
            }
        return {
            "status": "error",
            "message": "管理号在目标群缺少 add_admins 权限，无法批量设管",
            "promote": result,
        }
    return result


class InviteOneClickRequest(BaseModel):
    group_link: str
    targets: list[str] = Field(default_factory=list)
    session_ids: list[int] = Field(default_factory=list)
    rank: str = "Admin"
    join_delay_seconds: int = 1
    join_random_delay: bool = True
    promote_delay_seconds: int = 1
    promote_random_delay: bool = True
    invite_delay_seconds: int = 20
    invite_random_delay: bool = True
    max_per_account: int = 20
    grant_add_admins: bool = True
    bootstrap_admin_count: int = 5
    manager_session_ids: list[int] = Field(default_factory=list)


@router.post("/invite/one_click")
async def invite_one_click(payload: InviteOneClickRequest):
    group_link = payload.group_link.strip()
    unique_targets = list(dict.fromkeys([x.strip() for x in payload.targets if str(x).strip()]))
    if not group_link:
        return {"status": "error", "message": "Group link is required"}

    manager_ids = [int(x) for x in (payload.manager_session_ids or []) if str(x).isdigit()]
    if not manager_ids:
        manager_ids = await get_active_manager_session_ids()
    if not manager_ids:
        return {"status": "error", "message": "请先在账号管理页完成管理号登录"}

    default_participants = await get_active_non_manager_session_ids()
    requested_ids = [int(x) for x in (payload.session_ids or []) if str(x).isdigit()]
    join_ids = requested_ids if requested_ids else default_participants
    if not join_ids:
        join_ids = await get_active_session_ids()
    join_ids = [sid for sid in join_ids if sid not in manager_ids]
    if not join_ids:
        return {"status": "error", "message": "没有可执行加群的协议号"}

    join_result = await join_group_for_all_active_sessions(
        group_link,
        session_ids=join_ids,
        delay_seconds=payload.join_delay_seconds,
        random_delay=payload.join_random_delay,
    )
    join_success_ids = [
        item["session_id"]
        for item in (join_result.get("items") or [])
        if item.get("status") == "success" and str(item.get("session_id", "")).isdigit()
    ]
    promote_scope_ids = [int(x) for x in join_success_ids if str(x).isdigit()]
    if not promote_scope_ids:
        return {"status": "error", "message": "没有成功进群的账号，无法执行设管"}
    promote_result = await promote_all_sessions_to_admins(
        group_link,
        session_ids=promote_scope_ids,
        promoter_session_ids=manager_ids,
        rank=(payload.rank or "Admin").strip() or "Admin",
        delay_seconds=payload.promote_delay_seconds,
        random_delay=payload.promote_random_delay,
        grant_add_admins=payload.grant_add_admins,
        bootstrap_admin_count=payload.bootstrap_admin_count,
    )
    if int(promote_result.get("promoters") or 0) == 0:
        reason_stats = promote_result.get("reason_stats") or {}
        if int(reason_stats.get("UNKNOWN_ERROR") or 0) > 0:
            return {"status": "error", "message": "群链接解析失败，请改用有效的 @群用户名 或 https://t.me/+邀请链接", "promote": promote_result}
        return {"status": "error", "message": "管理号在目标群缺少 add_admins 权限，无法继续设管", "promote": promote_result}
    actual_manager_ids = set(
        int(x)
        for x in (promote_result.get("promoter_session_ids") or manager_ids or [])
        if str(x).isdigit()
    )
    invite_session_ids = [
        int(item["session_id"])
        for item in (promote_result.get("items") or [])
        if item.get("status") == "success"
        and str(item.get("session_id", "")).isdigit()
        and int(item["session_id"]) not in actual_manager_ids
    ]
    if not invite_session_ids:
        return {"status": "error", "message": "设管后没有可用于邀请的管理员账号"}

    invite_task = None
    if unique_targets:
        invite_task = await create_invite_task_record(
            group_link=group_link,
            targets=unique_targets,
            delay_seconds=payload.invite_delay_seconds,
            random_delay=payload.invite_random_delay,
            max_per_account=payload.max_per_account,
            invite_session_ids=invite_session_ids,
        )
    return {
        "status": "success",
        "join": join_result,
        "promote": promote_result,
        "invite_task": invite_task,
        "manager_session_ids": sorted(list(actual_manager_ids)),
        "promote_scope_ids": promote_scope_ids,
        "invite_session_ids": invite_session_ids,
    }


@router.get("/{task_id}/targets")
async def get_task_targets(task_id: int):
    rows = await fetch_all(
        """
        SELECT tt.*, s.phone as sender_phone 
        FROM task_targets tt
        LEFT JOIN sessions s ON tt.worker_session_id = s.id
        WHERE tt.task_id = ?
        ORDER BY tt.id ASC
        """, 
        (task_id,)
    )
    return {"items": rows}


@router.post("/{task_id}/stop")
async def stop_task(task_id: int):
    await execute("UPDATE tasks SET status = 'stopped' WHERE id = ? AND status = 'running'", (task_id,))
    return {"status": "stopped"}


@router.post("/{task_id}/restart")
async def restart_task(task_id: int):
    task = await fetch_all("SELECT status FROM tasks WHERE id = ?", (task_id,))
    if not task:
        return {"status": "error", "message": "Task not found"}
    if task[0]["status"] == "running":
        return {"status": "error", "message": "Task is already running"}
    await execute("UPDATE task_targets SET status = 'pending', error = NULL WHERE task_id = ? AND status = 'failed'", (task_id,))
    await execute("UPDATE task_targets SET executed_at = NULL WHERE task_id = ? AND status = 'pending'", (task_id,))
    await execute("UPDATE tasks SET status = 'queued' WHERE id = ?", (task_id,))
    asyncio.create_task(run_task(task_id))
    return {"status": "restarted"}


@router.delete("/{task_id}")
async def delete_task(task_id: int):
    # Clean up everything related to this task
    await execute("DELETE FROM task_targets WHERE task_id = ?", (task_id,))
    await execute("DELETE FROM logs WHERE task_id = ?", (task_id,))
    await execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    return {"status": "deleted"}


@router.get("")
async def list_tasks(task_type: str | None = None):
    if task_type:
        if task_type == "dm":
            rows = await fetch_all("SELECT * FROM tasks WHERE task_type = 'dm' OR task_type IS NULL ORDER BY id DESC")
        else:
            rows = await fetch_all("SELECT * FROM tasks WHERE task_type = ? ORDER BY id DESC", (task_type,))
    else:
        rows = await fetch_all("SELECT * FROM tasks ORDER BY id DESC")
    return {"items": rows}
