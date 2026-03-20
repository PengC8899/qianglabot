import asyncio
import json
import os
import random
import time
from urllib.parse import urlparse, parse_qs
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest, GetParticipantRequest, EditAdminRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.types import ChannelParticipantCreator, ChannelParticipantAdmin, ChatAdminRights, InputUser, InputPhoneContact, ChannelParticipantsRecent
from telethon.tl.functions.contacts import ImportContactsRequest
from database import get_db, fetch_one, fetch_all, execute, now_iso, deserialize_targets, SESSION_DIR
from utils import get_proxy_config


async def log_event(task_id, session_id, target, status, error=None):
    from logs import log_hub
    event_time = now_iso()
    await execute(
        """
        INSERT INTO logs (task_id, session_id, target, status, error, time)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (task_id, session_id, target, status, error, event_time),
    )
    await log_hub.broadcast(
        {
            "task_id": task_id,
            "session_id": session_id,
            "target": target,
            "status": status,
            "error": error,
            "time": event_time,
        },
        task_id=task_id,
    )


async def check_blacklist(target):
    row = await fetch_one("SELECT id FROM blacklist WHERE username = ?", (target,))
    return row is not None


def process_template(template):
    import re
    def replace(match):
        choices = match.group(1).split("|")
        return random.choice(choices)
    return re.sub(r"\{([^{}]+)\}", replace, template)


async def update_health_score(session_id, delta):
    await execute(
        "UPDATE sessions SET health_score = MAX(0, MIN(100, IFNULL(health_score, 100) + ?)) WHERE id = ?",
        (delta, session_id),
    )


async def human_like_behavior(client, target):
    try:
        await asyncio.sleep(random.uniform(1.0, 5.0))
        async with client.action(target, "typing"):
            await asyncio.sleep(random.uniform(2.0, 5.0))
    except Exception:
        pass


async def pick_api_key_for_send(session_row):
    key = await fetch_one(
        "SELECT id, api_id, api_hash FROM api_keys ORDER BY COALESCE(last_used_at, '') ASC, id ASC LIMIT 1"
    )
    if key:
        await execute("UPDATE api_keys SET last_used_at = ? WHERE id = ?", (now_iso(), key["id"]))
        return key["api_id"], key["api_hash"]
    return session_row["api_id"], session_row["api_hash"]


async def release_task_locks(task_id):
    await execute("UPDATE sessions SET current_task_id = NULL WHERE current_task_id = ?", (task_id,))


async def lock_one_session(task_id, task, attempted_ids, per_session_counts, allowed_session_ids=None):
    allowed_ids = [int(x) for x in (allowed_session_ids or []) if str(x).isdigit()]
    if allowed_session_ids is not None and not allowed_ids:
        return None
    if allowed_ids:
        placeholders = ",".join(["?"] * len(allowed_ids))
        rows = await fetch_all(
            f"""
            SELECT *
            FROM sessions
            WHERE status = 'active'
              AND id IN ({placeholders})
              AND (current_task_id IS NULL OR current_task_id = ?)
              AND (flood_wait IS NULL OR flood_wait <= ?)
            ORDER BY RANDOM()
            """,
            tuple(allowed_ids) + (task_id, int(time.time())),
        )
    else:
        rows = await fetch_all(
            """
            SELECT *
            FROM sessions
            WHERE status = 'active'
              AND (current_task_id IS NULL OR current_task_id = ?)
              AND (flood_wait IS NULL OR flood_wait <= ?)
            ORDER BY RANDOM()
            """,
            (task_id, int(time.time())),
        )
    for row in rows:
        sid = row["id"]
        if sid in attempted_ids:
            continue
        if per_session_counts.get(sid, 0) >= int(task["max_per_account"] or 1):
            continue
        await execute(
            "UPDATE sessions SET current_task_id = ? WHERE id = ? AND (current_task_id IS NULL OR current_task_id = ?)",
            (task_id, sid, task_id),
        )
        locked = await fetch_one("SELECT current_task_id FROM sessions WHERE id = ?", (sid,))
        if locked and locked["current_task_id"] == task_id:
            return row
    return None


def extract_group_target(group_link: str):
    raw = (group_link or "").strip()
    if not raw:
        return {"is_invite": False, "target": ""}
    if raw.startswith("tg://"):
        parsed_tg = urlparse(raw)
        domain = (parse_qs(parsed_tg.query).get("domain", [""])[0] or "").strip()
        if domain:
            raw = domain
    if raw.startswith("@"):
        raw = raw[1:]
    normalized = raw
    if normalized.startswith("t.me/") or normalized.startswith("telegram.me/"):
        normalized = f"https://{normalized}"
    if "://" in normalized:
        parsed = urlparse(normalized)
        host = (parsed.netloc or "").lower()
        if host in {"t.me", "www.t.me", "telegram.me", "www.telegram.me"}:
            normalized = (parsed.path or "").strip("/")
        else:
            normalized = (parsed.path or "").strip("/")
    normalized = normalized.strip().strip("/").split("?", 1)[0].split("#", 1)[0]
    if normalized.startswith("+"):
        return {"is_invite": True, "target": normalized[1:]}
    if normalized.startswith("joinchat/"):
        return {"is_invite": True, "target": normalized.split("joinchat/", 1)[1]}
    return {"is_invite": False, "target": normalized}


def classify_join_error(error_text: str):
    text = (error_text or "").upper()
    if "INVITE_HASH_EXPIRED" in text:
        return "INVITE_EXPIRED"
    if "INVITE_HASH_INVALID" in text:
        return "INVITE_INVALID"
    if "CHANNEL_PRIVATE" in text:
        return "GROUP_PRIVATE"
    if "CHANNELS_TOO_MUCH" in text:
        return "TOO_MANY_GROUPS"
    if "USER_BANNED_IN_CHANNEL" in text:
        return "BANNED_IN_GROUP"
    if "PEER_FLOOD" in text:
        return "PEER_FLOOD"
    if "FLOOD_WAIT" in text:
        return "FLOOD_WAIT"
    if "AUTH_KEY_UNREGISTERED" in text:
        return "SESSION_INVALID"
    if "SESSION PASSWORD NEEDED" in text:
        return "SESSION_2FA_REQUIRED"
    return "UNKNOWN_ERROR"


def classify_admin_error(error_text: str):
    text = (error_text or "").upper()
    if "CHAT_ADMIN_REQUIRED" in text:
        return "NO_PROMOTE_PERMISSION"
    if "RIGHT_FORBIDDEN" in text:
        return "RIGHT_FORBIDDEN"
    if "PARTICIPANT_ID_INVALID" in text:
        return "TARGET_ID_INVALID"
    if "PEER_ID_INVALID" in text:
        return "TARGET_ID_INVALID"
    if "INVALID OBJECT ID FOR A USER" in text:
        return "TARGET_ID_INVALID"
    if "INPUTUSERDEACTIVATED" in text:
        return "TARGET_ID_INVALID"
    if "TARGET_RESOLVE_FAILED" in text:
        return "TARGET_ID_INVALID"
    if "USER_NOT_PARTICIPANT" in text:
        return "TARGET_NOT_IN_GROUP"
    if "USER_PRIVACY_RESTRICTED" in text:
        return "TARGET_PRIVACY_RESTRICTED"
    if "FLOOD_WAIT" in text:
        return "FLOOD_WAIT"
    if "PEER_FLOOD" in text:
        return "PEER_FLOOD"
    if "ADMINS_TOO_MUCH" in text:
        return "ADMIN_LIMIT_REACHED"
    if "CHANNEL_PRIVATE" in text:
        return "GROUP_PRIVATE"
    return "UNKNOWN_ERROR"


async def build_client_from_session(session_row):
    api_id, api_hash = await pick_api_key_for_send(session_row)
    proxy = await get_proxy_config()
    if session_row.get("session_string"):
        return TelegramClient(StringSession(session_row["session_string"]), api_id, api_hash, proxy=proxy)
    session_path = os.path.join(SESSION_DIR, session_row["session_file"])
    return TelegramClient(session_path, api_id, api_hash, proxy=proxy)


def build_invite_admin_rights(add_admins=False):
    try:
        return ChatAdminRights(
            change_info=False,
            post_messages=False,
            edit_messages=False,
            delete_messages=False,
            ban_users=False,
            invite_users=True,
            pin_messages=True,
            add_admins=bool(add_admins),
            anonymous=False,
            manage_call=False,
            other=False,
            manage_topics=False,
        )
    except TypeError:
        return ChatAdminRights(
            change_info=False,
            post_messages=False,
            edit_messages=False,
            delete_messages=False,
            ban_users=False,
            invite_users=True,
            pin_messages=True,
            add_admins=bool(add_admins),
            anonymous=False,
            manage_call=False,
            other=False,
        )


async def can_promote_admin(client, group_entity):
    try:
        me = await client.get_me()
        participant = await client(GetParticipantRequest(channel=group_entity, participant=me.id))
        part = participant.participant
        print(f"[DEBUG] Session {me.id} in group {group_entity.id}: {type(part)}")
        if isinstance(part, ChannelParticipantCreator):
            return True
        if isinstance(part, ChannelParticipantAdmin):
            rights = getattr(part, "admin_rights", None)
            can_add = bool(rights and getattr(rights, "add_admins", False))
            print(f"[DEBUG] Session {me.id} is Admin. add_admins={can_add}")
            return can_add
        print(f"[DEBUG] Session {me.id} is NOT Creator or Admin")
        return False
    except Exception as e:
        print(f"[DEBUG] can_promote_admin error: {e}")
        return False


async def can_invite_members(client, group_entity):
    try:
        me = await client.get_me()
        participant = await client(GetParticipantRequest(channel=group_entity, participant=me.id))
        part = participant.participant
        if isinstance(part, ChannelParticipantCreator):
            return True
        if isinstance(part, ChannelParticipantAdmin):
            rights = getattr(part, "admin_rights", None)
            return bool(rights and getattr(rights, "invite_users", False))
        return False
    except Exception:
        return False


async def collect_invite_admin_session_ids(group_link: str, preferred_session_ids=None):
    now_ts = int(time.time())
    preferred_ids = [int(x) for x in (preferred_session_ids or []) if str(x).isdigit()]
    if preferred_ids:
        placeholders = ",".join(["?"] * len(preferred_ids))
        sessions = await fetch_all(
            f"SELECT * FROM sessions WHERE status = 'active' AND (flood_wait IS NULL OR flood_wait <= ?) AND id IN ({placeholders}) ORDER BY id ASC",
            (now_ts, *preferred_ids),
        )
    else:
        sessions = await fetch_all(
            "SELECT * FROM sessions WHERE status = 'active' AND (flood_wait IS NULL OR flood_wait <= ?) ORDER BY id ASC",
            (now_ts,),
        )
    if not sessions:
        return []
    eligible_ids = []
    eligible_lock = asyncio.Lock()
    queue = asyncio.Queue()
    for session_row in sessions:
        queue.put_nowait(session_row)

    worker_count = min(max(1, len(sessions)), 30)

    async def check_worker():
        while True:
            try:
                session_row = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            sid = session_row["id"]
            client = None
            try:
                client = await build_client_from_session(session_row)
                await client.connect()
                if not await client.is_user_authorized():
                    await execute("UPDATE sessions SET status = 'invalid' WHERE id = ?", (sid,))
                    continue
                try:
                    group_entity = await join_group_with_client(client, group_link)
                except errors.UserAlreadyParticipantError:
                    parsed = extract_group_target(group_link)
                    group_entity = await client.get_entity(parsed["target"] or group_link)
                if await can_invite_members(client, group_entity):
                    async with eligible_lock:
                        eligible_ids.append(sid)
            except Exception:
                continue
            finally:
                if client and client.is_connected():
                    await client.disconnect()
                queue.task_done()

    workers = [asyncio.create_task(check_worker()) for _ in range(worker_count)]
    await queue.join()
    for worker in workers:
        if not worker.done():
            worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    eligible_ids.sort()
    return eligible_ids


async def collect_joined_member_info(session_row, group_link: str):
    sid = session_row["id"]
    client = None
    try:
        client = await build_client_from_session(session_row)
        await client.connect()
        if not await client.is_user_authorized():
            await execute("UPDATE sessions SET status = 'invalid' WHERE id = ?", (sid,))
            return {"status": "failed", "error": "Session unauthorized", "error_code": "SESSION_UNAUTHORIZED"}
        try:
            await join_group_with_client(client, group_link)
        except errors.UserAlreadyParticipantError:
            pass
        me = await client.get_me()
        return {
            "status": "success",
            "user_id": me.id,
            "user_access_hash": getattr(me, "access_hash", None),
            "phone": session_row["phone"],
            "session_id": sid,
        }
    except errors.FloodWaitError as e:
        wait_seconds = int(e.seconds or 0)
        await execute("UPDATE sessions SET flood_wait = ? WHERE id = ?", (int(time.time()) + wait_seconds, sid))
        return {"status": "failed", "error": f"Flood wait {wait_seconds}s", "error_code": "FLOOD_WAIT"}
    except errors.RPCError as e:
        err = str(e)
        return {"status": "failed", "error": err, "error_code": classify_join_error(err)}
    except Exception as e:
        err = str(e)
        return {"status": "failed", "error": err, "error_code": classify_join_error(err)}
    finally:
        if client and client.is_connected():
            await client.disconnect()


async def promote_all_sessions_to_admins(
    group_link: str,
    session_ids=None,
    promoter_session_ids=None,
    rank="Admin",
    delay_seconds=1,
    random_delay=True,
    grant_add_admins=True,
    bootstrap_admin_count=5,
):
    ids = [int(x) for x in (session_ids or []) if str(x).isdigit()]
    if ids:
        placeholders = ",".join(["?"] * len(ids))
        sessions = await fetch_all(f"SELECT * FROM sessions WHERE id IN ({placeholders}) ORDER BY id ASC", tuple(ids))
    else:
        sessions = await fetch_all("SELECT * FROM sessions ORDER BY id ASC")
    if not sessions:
        return {"total": 0, "success": 0, "failed": 0, "processed": 0, "promoters": 0, "items": [], "reason_stats": {}}

    results = []
    reason_stats = {}
    member_infos = []
    now_ts = int(time.time())
    base_delay = max(0, int(delay_seconds or 0))

    def add_result(item):
        results.append(item)
        if item["status"] != "success":
            code = item.get("error_code") or "UNKNOWN_ERROR"
            reason_stats[code] = reason_stats.get(code, 0) + 1

    for session_row in sessions:
        sid = session_row["id"]
        if session_row.get("status") != "active":
            add_result({"session_id": sid, "phone": session_row["phone"], "status": "failed", "error": "Session not active", "error_code": "SESSION_NOT_ACTIVE"})
            continue
        if session_row.get("flood_wait") and int(session_row["flood_wait"]) > now_ts:
            remain = int(session_row["flood_wait"]) - now_ts
            add_result({"session_id": sid, "phone": session_row["phone"], "status": "failed", "error": f"Session in flood wait {remain}s", "error_code": "SESSION_FLOOD_WAIT"})
            continue
        info = await collect_joined_member_info(session_row, group_link)
        if info["status"] != "success":
            add_result({"session_id": sid, "phone": session_row["phone"], "status": "failed", "error": info.get("error"), "error_code": info.get("error_code")})
        else:
            member_infos.append({
                "session_row": session_row,
                "user_id": info["user_id"],
                "user_access_hash": info.get("user_access_hash"),
                "session_id": sid,
                "phone": session_row["phone"],
            })
        if base_delay > 0:
            sleep_s = random.uniform(base_delay * 0.6, base_delay * 1.4) if random_delay else base_delay
            await asyncio.sleep(sleep_s)

    promoter_ids = [int(x) for x in (promoter_session_ids or []) if str(x).isdigit()]
    if promoter_ids:
        placeholders = ",".join(["?"] * len(promoter_ids))
        promoter_candidates = await fetch_all(
            f"SELECT * FROM sessions WHERE status = 'active' AND (flood_wait IS NULL OR flood_wait <= ?) AND id IN ({placeholders}) ORDER BY id ASC",
            (now_ts, *promoter_ids),
        )
    else:
        promoter_candidates = await fetch_all(
            "SELECT * FROM sessions WHERE status = 'active' AND (flood_wait IS NULL OR flood_wait <= ?) ORDER BY id ASC",
            (now_ts,),
        )
    target_candidate_ids = {x["session_id"] for x in member_infos}
    candidate_rows = []
    for row in promoter_candidates:
        if row["id"] in target_candidate_ids:
            candidate_rows.append(row)
    for row in promoter_candidates:
        if row["id"] not in target_candidate_ids:
            candidate_rows.append(row)

    promoters = []
    for candidate_row in candidate_rows:
        client = None
        try:
            print(f"[DEBUG] Checking candidate session {candidate_row['id']}...")
            client = await build_client_from_session(candidate_row)
            await client.connect()
            if not await client.is_user_authorized():
                print(f"[DEBUG] Candidate session {candidate_row['id']} unauthorized")
                await execute("UPDATE sessions SET status = 'invalid' WHERE id = ?", (candidate_row["id"],))
                continue
            try:
                print(f"[DEBUG] Candidate session {candidate_row['id']} joining group {group_link}...")
                group_entity = await join_group_with_client(client, group_link)
            except errors.UserAlreadyParticipantError:
                print(f"[DEBUG] Candidate session {candidate_row['id']} already in group")
                parsed = extract_group_target(group_link)
                group_entity = await client.get_entity(parsed["target"] or group_link)
            except Exception as e:
                print(f"[DEBUG] Candidate session {candidate_row['id']} failed to join/get group: {e}")
                continue
                
            if await can_promote_admin(client, group_entity):
                print(f"[DEBUG] Session {candidate_row['id']} CAN promote admin")
                promoters.append({"client": client, "group_entity": group_entity, "session_id": candidate_row["id"]})
                client = None
            else:
                print(f"[DEBUG] Session {candidate_row['id']} CANNOT promote admin")
        except Exception as e:
            print(f"[DEBUG] Error checking promoter {candidate_row['id']}: {e}")
            pass
        finally:
            if client and client.is_connected():
                await client.disconnect()

    if not promoters:
        for info in member_infos:
            add_result({
                "session_id": info["session_id"],
                "phone": info["phone"],
                "status": "failed",
                "error": "No account has add_admins permission",
                "error_code": "NO_PROMOTER_PERMISSION",
            })
        success_count = sum(1 for x in results if x["status"] == "success")
        results.sort(key=lambda x: x["session_id"])
        return {
            "total": len(sessions),
            "success": success_count,
            "failed": len(sessions) - success_count,
            "processed": len(member_infos),
            "promoters": 0,
            "promoter_session_ids": [],
            "items": results,
            "reason_stats": reason_stats,
        }

    bootstrap_count = max(0, int(bootstrap_admin_count or 0))
    rights_with_add_admins = build_invite_admin_rights(add_admins=True)
    rights_without_add_admins = build_invite_admin_rights(add_admins=False)
    promoter_session_ids = {x["session_id"] for x in promoters}

    async def build_target_candidates(client, group_entity, user_id: int, user_access_hash, phone: str = None):
        candidate_targets = []

        # 1. Try to get entity from local cache
        try:
            resolved_user = await client.get_entity(user_id)
            resolved_hash = getattr(resolved_user, "access_hash", None)
            if resolved_hash is not None:
                candidate_targets.append(InputUser(user_id=user_id, access_hash=resolved_hash))
        except Exception:
            pass

        # 2. Try by phone number if available
        if phone:
            try:
                phone_str = phone if phone.startswith("+") else f"+{phone}"
                resolved_user = await client.get_entity(phone_str)
                resolved_hash = getattr(resolved_user, "access_hash", None)
                if resolved_hash is not None:
                    candidate_targets.append(InputUser(user_id=user_id, access_hash=resolved_hash))
            except Exception:
                pass
                
            # 2.5 Try adding contact by phone number to force server to resolve
            try:
                phone_str = phone if phone.startswith("+") else f"+{phone}"
                contact = InputPhoneContact(client_id=0, phone=phone_str, first_name="User", last_name="")
                result = await client(ImportContactsRequest([contact]))
                if result.users:
                    for u in result.users:
                        if int(getattr(u, "id", 0) or 0) == int(user_id):
                            resolved_hash = getattr(u, "access_hash", None)
                            if resolved_hash is not None:
                                candidate_targets.append(InputUser(user_id=user_id, access_hash=resolved_hash))
            except Exception as e:
                print(f"[DEBUG] ImportContactsRequest failed: {e}")

        # 3. Try to find the user in the recent participants of the group
        try:
            # Fetch recent participants (limit to 200 to be safe and fast)
            participants = await client.get_participants(group_entity, limit=200, filter=ChannelParticipantsRecent())
            for user in participants:
                if int(getattr(user, "id", 0) or 0) == int(user_id):
                    participant_hash = getattr(user, "access_hash", None)
                    if participant_hash is not None:
                        candidate_targets.append(InputUser(user_id=user_id, access_hash=participant_hash))
                    break
        except Exception as e:
            print(f"[DEBUG] get_participants failed for {user_id}: {e}")

        # 4. Try GetParticipantRequest using the access_hash we have (it bypasses local cache check, might fail at Telegram side but worth trying)
        try:
            input_peer = InputUser(user_id=user_id, access_hash=user_access_hash or 0)
            participant = await client(GetParticipantRequest(channel=group_entity, participant=input_peer))
            participant_user = None
            for user in getattr(participant, "users", []) or []:
                if int(getattr(user, "id", 0) or 0) == int(user_id):
                    participant_user = user
                    break
            participant_hash = getattr(participant_user, "access_hash", None) if participant_user else None
            if participant_hash is not None:
                candidate_targets.append(InputUser(user_id=user_id, access_hash=participant_hash))
        except Exception:
            pass

        # 5. Fallback to the original access hash
        if user_access_hash is not None:
            candidate_targets.append(InputUser(user_id=user_id, access_hash=user_access_hash))

        dedup = {}
        for candidate in candidate_targets:
            key = f"{int(getattr(candidate, 'user_id', 0) or 0)}:{int(getattr(candidate, 'access_hash', 0) or 0)}"
            dedup[key] = candidate
        return list(dedup.values())

    promoter_index = 0
    for idx, info in enumerate(member_infos):
        sid = info["session_id"]
        phone = info["phone"]
        promoter = promoters[promoter_index % len(promoters)]
        promoter_index += 1
        client = promoter["client"]
        group_entity = promoter["group_entity"]
        try:
            current_rights = rights_without_add_admins
            if grant_add_admins and idx < bootstrap_count:
                current_rights = rights_with_add_admins

            candidate_targets = await build_target_candidates(
                client,
                group_entity,
                int(info["user_id"]),
                info.get("user_access_hash"),
                info.get("phone")
            )
            if not candidate_targets:
                raise ValueError(f"TARGET_RESOLVE_FAILED:{info['user_id']}")

            promoted = False
            last_error = None
            for candidate in candidate_targets:
                try:
                    await client(EditAdminRequest(channel=group_entity, user_id=candidate, admin_rights=current_rights, rank=rank))
                    promoted = True
                    break
                except errors.RPCError as e:
                    last_error = e
                    err_upper = str(e).upper()
                    if "PARTICIPANT_ID_INVALID" in err_upper or "PEER_ID_INVALID" in err_upper or "COULD NOT FIND THE INPUT ENTITY" in err_upper or "INVALID OBJECT ID FOR A USER" in err_upper or "USER_ID_INVALID" in err_upper:
                        continue
                    raise

            if not promoted and last_error:
                raise last_error
            if not promoted:
                raise ValueError("TARGET_RESOLVE_FAILED")

            add_result({"session_id": sid, "phone": phone, "status": "success", "error": None, "error_code": "PROMOTED"})
            if grant_add_admins and idx < bootstrap_count and sid not in promoter_session_ids:
                new_client = None
                try:
                    new_client = await build_client_from_session(info["session_row"])
                    await new_client.connect()
                    if await new_client.is_user_authorized():
                        parsed = extract_group_target(group_link)
                        new_group_entity = await new_client.get_entity(parsed["target"] or group_link)
                        if await can_promote_admin(new_client, new_group_entity):
                            promoters.append({"client": new_client, "group_entity": new_group_entity, "session_id": sid})
                            promoter_session_ids.add(sid)
                            new_client = None
                except Exception:
                    pass
                finally:
                    if new_client and new_client.is_connected():
                        await new_client.disconnect()
        except errors.RPCError as e:
            err = str(e)
            add_result({"session_id": sid, "phone": phone, "status": "failed", "error": err, "error_code": classify_admin_error(err)})
        except Exception as e:
            err = str(e)
            add_result({"session_id": sid, "phone": phone, "status": "failed", "error": err, "error_code": classify_admin_error(err)})
        if base_delay > 0:
            sleep_s = random.uniform(base_delay * 0.6, base_delay * 1.4) if random_delay else base_delay
            await asyncio.sleep(sleep_s)

    for promoter in promoters:
        client = promoter["client"]
        if client and client.is_connected():
            await client.disconnect()

    success_count = sum(1 for x in results if x["status"] == "success")
    results.sort(key=lambda x: x["session_id"])
    return {
        "total": len(sessions),
        "success": success_count,
        "failed": len(sessions) - success_count,
        "processed": len(member_infos),
        "promoters": len(promoters),
        "promoter_session_ids": sorted([int(x["session_id"]) for x in promoters if str(x.get("session_id", "")).isdigit()]),
        "items": results,
        "reason_stats": reason_stats,
    }


async def join_group_with_client(client, group_link: str):
    parsed = extract_group_target(group_link)
    if not parsed["target"]:
        raise ValueError("Group link is empty")
    
    if parsed["is_invite"]:
        try:
            updates = await client(ImportChatInviteRequest(parsed["target"]))
            if getattr(updates, "chats", None):
                return updates.chats[0]
            # Try to get entity if no chats returned (unlikely)
            return await client.get_entity(group_link)
        except errors.UserAlreadyParticipantError:
            print(f"[DEBUG] User already in group (invite link). Finding entity via CheckChatInviteRequest...")
            try:
                invite = await client(CheckChatInviteRequest(parsed["target"]))
                chat_id = getattr(invite.chat, "id", None)
                if not chat_id:
                    raise ValueError("CheckChatInviteRequest returned no chat ID")
                
                # Iterate dialogs to find exact entity (needed for further actions)
                async for dialog in client.iter_dialogs():
                    # Check for direct ID match or -100 prefix match
                    if dialog.id == chat_id or dialog.id == int(f"-100{chat_id}") or dialog.id == int(f"-{chat_id}"):
                        return dialog.entity
                
                # If not found in dialogs (weird if participant), return the partial chat object
                return invite.chat
            except Exception as e:
                print(f"[DEBUG] Failed to resolve entity via CheckChatInviteRequest: {e}")
                raise
        except Exception as e:
            # Other errors for ImportChatInviteRequest
            print(f"[DEBUG] ImportChatInviteRequest failed: {e}")
            raise

    # Public link logic
    try:
        await client(JoinChannelRequest(parsed["target"]))
    except errors.UserAlreadyParticipantError:
        pass
    except Exception as e:
        print(f"[DEBUG] JoinChannelRequest failed: {e}")
        # Continue to get_entity attempt
        pass
        
    return await client.get_entity(parsed["target"])


async def join_group_with_session(session_row, group_link: str):
    sid = session_row["id"]
    client = None
    try:
        api_id, api_hash = await pick_api_key_for_send(session_row)
        proxy = await get_proxy_config()
        if session_row.get("session_string"):
            client = TelegramClient(StringSession(session_row["session_string"]), api_id, api_hash, proxy=proxy)
        else:
            session_path = os.path.join(SESSION_DIR, session_row["session_file"])
            client = TelegramClient(session_path, api_id, api_hash, proxy=proxy)
        await client.connect()
        if not await client.is_user_authorized():
            await execute("UPDATE sessions SET status = 'invalid' WHERE id = ?", (sid,))
            return {"status": "failed", "error": "Session unauthorized", "error_code": "SESSION_UNAUTHORIZED"}
        try:
            await join_group_with_client(client, group_link)
            await update_health_score(sid, 1)
            return {"status": "success", "error": None, "error_code": None}
        except errors.UserAlreadyParticipantError:
            return {"status": "success", "error": "Already participant", "error_code": "ALREADY_PARTICIPANT"}
        except errors.FloodWaitError as e:
            wait_seconds = int(e.seconds or 0)
            await execute("UPDATE sessions SET flood_wait = ? WHERE id = ?", (int(time.time()) + wait_seconds, sid))
            await update_health_score(sid, -10 if wait_seconds < 300 else -20)
            return {"status": "failed", "error": f"Flood wait {wait_seconds}s", "error_code": "FLOOD_WAIT"}
        except errors.RPCError as e:
            await update_health_score(sid, -3)
            err = str(e)
            return {"status": "failed", "error": err, "error_code": classify_join_error(err)}
    except Exception as e:
        err = str(e)
        return {"status": "failed", "error": err, "error_code": classify_join_error(err)}
    finally:
        if client and client.is_connected():
            await client.disconnect()


async def join_group_for_all_active_sessions(group_link: str, session_ids=None, delay_seconds=2, random_delay=True):
    ids = [int(x) for x in (session_ids or []) if str(x).isdigit()]
    if ids:
        placeholders = ",".join(["?"] * len(ids))
        sessions = await fetch_all(f"SELECT * FROM sessions WHERE id IN ({placeholders}) ORDER BY id ASC", tuple(ids))
    else:
        sessions = await fetch_all("SELECT * FROM sessions ORDER BY id ASC")
    if not sessions:
        return {"total": 0, "success": 0, "failed": 0, "processed": 0, "items": [], "reason_stats": {}}
    results = []
    success_count = 0
    reason_stats = {}
    processed_count = 0
    result_lock = asyncio.Lock()
    now_ts = int(time.time())
    base_delay = max(0, int(delay_seconds or 0))
    queue = asyncio.Queue()
    for session_row in sessions:
        queue.put_nowait(session_row)

    worker_count = min(max(1, len(sessions)), 20)

    async def join_worker():
        nonlocal success_count, processed_count
        while True:
            try:
                session_row = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            sid = session_row["id"]
            try:
                if session_row.get("status") != "active":
                    result = {"status": "failed", "error": "Session not active", "error_code": "SESSION_NOT_ACTIVE"}
                elif session_row.get("flood_wait") and int(session_row["flood_wait"]) > now_ts:
                    remain = int(session_row["flood_wait"]) - now_ts
                    result = {"status": "failed", "error": f"Session in flood wait {remain}s", "error_code": "SESSION_FLOOD_WAIT"}
                else:
                    result = await join_group_with_session(session_row, group_link)
                    async with result_lock:
                        processed_count += 1
                async with result_lock:
                    if result["status"] == "success":
                        success_count += 1
                    else:
                        code = result.get("error_code") or "UNKNOWN_ERROR"
                        reason_stats[code] = reason_stats.get(code, 0) + 1
                    results.append({
                        "session_id": sid,
                        "phone": session_row["phone"],
                        "status": result["status"],
                        "error": result.get("error"),
                        "error_code": result.get("error_code"),
                    })
            finally:
                queue.task_done()
            if base_delay > 0:
                sleep_s = random.uniform(base_delay * 0.6, base_delay * 1.4) if random_delay else base_delay
                await asyncio.sleep(sleep_s)

    workers = [asyncio.create_task(join_worker()) for _ in range(worker_count)]
    await queue.join()
    for worker in workers:
        if not worker.done():
            worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    results.sort(key=lambda x: x["session_id"])
    return {
        "total": len(sessions),
        "success": success_count,
        "failed": len(sessions) - success_count,
        "processed": processed_count,
        "items": results,
        "reason_stats": reason_stats,
    }


async def check_all_sessions_in_group(group_link: str):
    sessions = await fetch_all("SELECT * FROM sessions WHERE status = 'active' ORDER BY id ASC")
    if not sessions:
        return {"items": []}

    results = []
    result_lock = asyncio.Lock()
    queue = asyncio.Queue()
    for session_row in sessions:
        queue.put_nowait(session_row)

    worker_count = min(max(1, len(sessions)), 30)

    async def check_worker():
        while True:
            try:
                session_row = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            
            sid = session_row["id"]
            phone = session_row["phone"]
            client = None
            status = "not_in_group"
            is_admin = False
            can_invite = False
            error = None
            
            try:
                client = await build_client_from_session(session_row)
                await client.connect()
                if not await client.is_user_authorized():
                    await execute("UPDATE sessions SET status = 'invalid' WHERE id = ?", (sid,))
                    status = "invalid_session"
                else:
                    try:
                        parsed = extract_group_target(group_link)
                        group_entity = await client.get_entity(parsed["target"] or group_link)
                        me = await client.get_me()
                        participant = await client(GetParticipantRequest(channel=group_entity, participant=me.id))
                        part = participant.participant
                        
                        status = "member"
                        if isinstance(part, ChannelParticipantCreator):
                            status = "creator"
                            is_admin = True
                            can_invite = True
                        elif isinstance(part, ChannelParticipantAdmin):
                            status = "admin"
                            is_admin = True
                            rights = getattr(part, "admin_rights", None)
                            can_invite = bool(rights and getattr(rights, "invite_users", False))
                    except errors.UserNotParticipantError:
                        status = "not_in_group"
                    except errors.ChannelPrivateError:
                        status = "not_in_group" # or cannot access
                        error = "Channel private / not joined"
                    except Exception as e:
                        error = str(e)
                        status = "error"
            except Exception as e:
                error = str(e)
                status = "error"
            finally:
                if client and client.is_connected():
                    await client.disconnect()
                
                async with result_lock:
                    results.append({
                        "session_id": sid,
                        "phone": phone,
                        "status": status,
                        "is_admin": is_admin,
                        "can_invite": can_invite,
                        "error": error
                    })
                queue.task_done()

    workers = [asyncio.create_task(check_worker()) for _ in range(worker_count)]
    await queue.join()
    for worker in workers:
        if not worker.done():
            worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    
    results.sort(key=lambda x: x["session_id"])
    return {"items": results}


async def send_once(task, task_id, target, target_db_id, session_row, per_session_counts, joined_group_entities):
    sid = session_row["id"]
    client = None
    try:
        api_id, api_hash = await pick_api_key_for_send(session_row)
        proxy = await get_proxy_config()
        if session_row.get("session_string"):
            client = TelegramClient(StringSession(session_row["session_string"]), api_id, api_hash, proxy=proxy)
        else:
            session_path = os.path.join(SESSION_DIR, session_row["session_file"])
            client = TelegramClient(session_path, api_id, api_hash, proxy=proxy)

        await client.connect()
        if not await client.is_user_authorized():
            await update_health_score(sid, -50)
            await execute("UPDATE sessions SET status = 'invalid' WHERE id = ?", (sid,))
            await log_event(task_id, sid, target, "failed", "Session unauthorized")
            return "retry:Session unauthorized"

        await human_like_behavior(client, target)
        task_type = task.get("task_type") or "dm"
        if task_type == "invite":
            group_link = (task.get("group_link") or "").strip()
            if not group_link:
                raise ValueError("Invite task missing group link")
            group_entity = joined_group_entities.get(sid)
            if not group_entity:
                try:
                    group_entity = await join_group_with_client(client, group_link)
                except errors.UserAlreadyParticipantError:
                    parsed = extract_group_target(group_link)
                    group_entity = await client.get_entity(parsed["target"] or group_link)
                joined_group_entities[sid] = group_entity
            target_entity = target
            if isinstance(target, str) and not target.isdigit() and not target.startswith("+"):
                target_entity = await client.get_entity(target)
            await client(InviteToChannelRequest(channel=group_entity, users=[target_entity]))
        else:
            real_target = target
            if isinstance(target, str) and not target.isdigit() and not target.startswith("+"):
                real_target = await client.get_entity(target)
            msg_content = process_template(task["message"])
            await client.send_message(real_target, msg_content)

        await log_event(task_id, sid, target, "success")
        per_session_counts[sid] = per_session_counts.get(sid, 0) + 1
        await update_health_score(sid, 1)
        await execute("UPDATE tasks SET success_count = success_count + 1 WHERE id = ?", (task_id,))
        await execute(
            "UPDATE task_targets SET status = 'success', worker_session_id = ?, executed_at = ? WHERE id = ?",
            (sid, now_iso(), target_db_id),
        )
        return "success"
    except errors.FloodWaitError as e:
        wait_seconds = int(e.seconds or 0)
        await log_event(task_id, sid, target, "flood_wait", f"Wait {wait_seconds}s")
        await execute("UPDATE sessions SET flood_wait = ? WHERE id = ?", (int(time.time()) + wait_seconds, sid))
        await update_health_score(sid, -10 if wait_seconds < 300 else -20)
        return f"retry:Wait {wait_seconds}s"
    except (errors.ApiIdInvalidError, errors.ApiIdPublishedFloodError):
        await log_event(task_id, sid, target, "api_error", "API key unavailable")
        return "retry:API key unavailable"
    except errors.UserPrivacyRestrictedError:
        await log_event(task_id, sid, target, "failed", "Privacy restricted")
        await update_health_score(sid, -1)
        await execute("UPDATE tasks SET fail_count = fail_count + 1 WHERE id = ?", (task_id,))
        await execute(
            "UPDATE task_targets SET status = 'failed', error = 'Privacy restricted', executed_at = ? WHERE id = ?",
            (now_iso(), target_db_id),
        )
        return "failed"
    except errors.UserAlreadyParticipantError:
        await log_event(task_id, sid, target, "success", "Already in group")
        per_session_counts[sid] = per_session_counts.get(sid, 0) + 1
        await execute("UPDATE tasks SET success_count = success_count + 1 WHERE id = ?", (task_id,))
        await execute(
            "UPDATE task_targets SET status = 'success', worker_session_id = ?, executed_at = ?, error = 'Already in group' WHERE id = ?",
            (sid, now_iso(), target_db_id),
        )
        return "success"
    except errors.RPCError as e:
        err_text = str(e)
        err_code = err_text.upper()
        if any(flag in err_code for flag in ["CHAT_ADMIN_REQUIRED", "USER_CHANNELS_TOO_MUCH", "USER_NOT_MUTUAL_CONTACT", "CHAT_WRITE_FORBIDDEN"]):
            await log_event(task_id, sid, target, "failed", err_text)
            await update_health_score(sid, -2)
            await execute("UPDATE tasks SET fail_count = fail_count + 1 WHERE id = ?", (task_id,))
            await execute(
                "UPDATE task_targets SET status = 'failed', error = ?, executed_at = ? WHERE id = ?",
                (err_text, now_iso(), target_db_id),
            )
            return "failed"
        await log_event(task_id, sid, target, "failed", err_text)
        await update_health_score(sid, -5)
        return f"retry:{err_text}"
    except Exception as e:
        err_text = str(e)
        await log_event(task_id, sid, target, "failed", err_text)
        return f"retry:{err_text}"
    finally:
        if client and client.is_connected():
            await client.disconnect()
        await execute("UPDATE sessions SET current_task_id = NULL WHERE id = ? AND current_task_id = ?", (sid, task_id))


async def run_task(task_id: int):
    task = await fetch_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if not task:
        return

    await execute("UPDATE tasks SET status = 'running' WHERE id = ?", (task_id,))
    await release_task_locks(task_id)

    total_count = await fetch_one("SELECT count(*) as cnt FROM task_targets WHERE task_id = ?", (task_id,))
    if total_count and total_count["cnt"] > 0:
        await execute("UPDATE tasks SET total_count = ? WHERE id = ?", (total_count["cnt"], task_id))
    else:
        try:
            targets_json = deserialize_targets(task["targets"])
            if targets_json and not await fetch_one("SELECT id FROM task_targets WHERE task_id = ?", (task_id,)):
                async with get_db() as db:
                    for t in targets_json:
                        await db.execute(
                            "INSERT INTO task_targets (task_id, target, status) VALUES (?, ?, 'pending')",
                            (task_id, t),
                        )
                    await db.commit()
                await execute("UPDATE tasks SET total_count = ? WHERE id = ?", (len(targets_json), task_id))
        except Exception:
            pass

    pending_targets = await fetch_all(
        "SELECT * FROM task_targets WHERE task_id = ? AND status = 'pending' ORDER BY id ASC",
        (task_id,),
    )
    if not pending_targets:
        await execute("UPDATE tasks SET status = 'completed' WHERE id = ?", (task_id,))
        return

    per_session_counts = {}
    joined_group_entities = {}
    task_type = task.get("task_type") or "dm"
    max_target_failures = 1 if task_type == "invite" else 10
    base_delay = max(1, int(task["delay_seconds"] or 30))
    invite_session_ids = None

    if task_type == "invite":
        group_link = (task.get("group_link") or "").strip()
        if not group_link:
            await log_event(task_id, None, "system", "failed", "Invite task missing group link")
            await execute("UPDATE tasks SET status = 'failed' WHERE id = ?", (task_id,))
            return
        preferred_ids = []
        raw_allowed_ids = task.get("allowed_session_ids")
        if raw_allowed_ids:
            try:
                parsed_ids = json.loads(raw_allowed_ids)
                preferred_ids = [int(x) for x in (parsed_ids or []) if str(x).isdigit()]
            except Exception:
                preferred_ids = []
        invite_session_ids = await collect_invite_admin_session_ids(group_link, preferred_ids)
        if not invite_session_ids:
            await log_event(task_id, None, "system", "failed", "No admin sessions with invite permission in allowed scope")
            await execute("UPDATE tasks SET status = 'failed' WHERE id = ?", (task_id,))
            return
        await log_event(task_id, None, "system", "info", f"Using {len(invite_session_ids)} admin sessions for invite task")

    async def process_target_row(target_row):
        state = await fetch_one("SELECT status FROM tasks WHERE id = ?", (task_id,))
        if not state or state["status"] == "stopped":
            return

        target = target_row["target"]
        target_db_id = target_row["id"]
        target_fail_count = 0
        target_last_error = None

        current_status = await fetch_one("SELECT status FROM task_targets WHERE id = ?", (target_db_id,))
        if not current_status or current_status["status"] != "pending":
            return

        if await check_blacklist(target):
            await log_event(task_id, None, target, "skipped", "blacklisted")
            await execute(
                "UPDATE task_targets SET status = 'skipped', error = 'blacklisted', executed_at = ? WHERE id = ?",
                (now_iso(), target_db_id),
            )
            return

        attempted_ids = set()
        done_for_target = False

        no_session_retry_count = 0
        MAX_NO_SESSION_RETRIES = 10

        while not done_for_target and target_fail_count < max_target_failures:
            # Check global task state occasionally
            state = await fetch_one("SELECT status FROM tasks WHERE id = ?", (task_id,))
            if not state or state["status"] == "stopped":
                done_for_target = True
                break

            session_row = await lock_one_session(task_id, task, attempted_ids, per_session_counts, invite_session_ids)
            if not session_row:
                no_session_retry_count += 1
                if no_session_retry_count > MAX_NO_SESSION_RETRIES:
                    target_last_error = "Resource exhausted (no available sessions)"
                    break
                
                # Check if we have ANY active sessions at all to decide if we should wait or fail hard
                if invite_session_ids is not None:
                    active_cnt = {"cnt": len(invite_session_ids)}
                else:
                    active_cnt = await fetch_one("SELECT count(*) as cnt FROM sessions WHERE status = 'active'")
                
                if active_cnt and active_cnt["cnt"] > 0:
                    # Busy wait with jitter to avoid thundering herd
                    await log_event(task_id, None, "system", "waiting", f"All sessions busy (retry {no_session_retry_count}/{MAX_NO_SESSION_RETRIES})")
                    await asyncio.sleep(random.uniform(2, 5))
                    continue
                
                # No active sessions at all
                await log_event(task_id, None, "system", "failed", "No active sessions")
                await execute("UPDATE tasks SET status = 'failed' WHERE id = ?", (task_id,))
                return

            # Got session, reset no-session counter
            no_session_retry_count = 0
            attempted_ids.add(session_row["id"])
            
            try:
                # Add timeout protection for single target processing
                send_result = await asyncio.wait_for(
                    send_once(task, task_id, target, target_db_id, session_row, per_session_counts, joined_group_entities),
                    timeout=300 # 5 minutes max per attempt
                )
            except asyncio.TimeoutError:
                send_result = f"retry:Timeout processing target"
                await log_event(task_id, session_row["id"], target, "timeout", "Operation timed out")
            except Exception as e:
                send_result = f"retry:Unexpected error: {str(e)}"
                await log_event(task_id, session_row["id"], target, "error", str(e))

            if send_result == "success":
                done_for_target = True
            elif send_result == "failed":
                done_for_target = True
            else:
                if isinstance(send_result, str) and send_result.startswith("retry:"):
                    target_last_error = send_result.split(":", 1)[1].strip() or "retry"
                target_fail_count += 1

        if not done_for_target:
            fail_reason = target_last_error or "Max retry limit reached"
            await execute(
                "UPDATE task_targets SET status = 'failed', error = ?, executed_at = ? WHERE id = ?",
                (fail_reason, now_iso(), target_db_id),
            )
            await execute("UPDATE tasks SET fail_count = fail_count + 1 WHERE id = ?", (task_id,))

        delay = random.uniform(base_delay * 0.8, base_delay * 1.5) if task["random_delay"] else base_delay
        await asyncio.sleep(delay)

    try:
        if task_type == "invite":
            worker_count = min(max(1, len(invite_session_ids)), 60)
        else:
            active_cnt = await fetch_one(
                "SELECT count(*) as cnt FROM sessions WHERE status = 'active' AND (flood_wait IS NULL OR flood_wait <= ?)",
                (int(time.time()),),
            )
            worker_count = min(max(1, int((active_cnt or {}).get("cnt") or 1)), 60)

        queue = asyncio.Queue()
        for row in pending_targets:
            queue.put_nowait(row)

        async def task_worker():
            while True:
                state = await fetch_one("SELECT status FROM tasks WHERE id = ?", (task_id,))
                if not state or state["status"] == "stopped":
                    return
                try:
                    target_row = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    await process_target_row(target_row)
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(task_worker()) for _ in range(worker_count)]
        await queue.join()
        for w in workers:
            if not w.done():
                w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

        state = await fetch_one("SELECT status FROM tasks WHERE id = ?", (task_id,))
        if state and state["status"] != "stopped":
            remain = await fetch_one("SELECT count(*) as cnt FROM task_targets WHERE task_id = ? AND status = 'pending'", (task_id,))
            if remain and remain["cnt"] == 0:
                await execute("UPDATE tasks SET status = 'completed' WHERE id = ?", (task_id,))
    finally:
        await release_task_locks(task_id)
