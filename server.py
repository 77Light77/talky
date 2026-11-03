"""Talky server v2.1.

Accounts, friend requests, direct messages, servers with text + voice
channels, images, stickers, and a voice relay.

Run locally:   uvicorn server:app --host 0.0.0.0 --port 8000

Saving data:
  - If the DATABASE_URL environment variable is set, everything is saved in
    that Postgres database (this survives restarts on free hosting).
  - Otherwise everything is saved in a local "data" folder (fine on your own
    computer, but free hosts like Render wipe it when the server restarts).
"""
import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import secrets
import time
import urllib.request
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response, WebSocket

# --------------------------------------------------------------- settings
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
DATA_DIR = os.environ.get("DATA_DIR", "data")
IMG_DIR = os.path.join(DATA_DIR, "images")

NAME_RE = re.compile(r"^[a-z0-9_]{2,20}$")
CHANNEL_RE = re.compile(r"^[a-z0-9-]{1,24}$")
IMAGE_ID_RE = re.compile(r"^[0-9a-f]{16}$")

MAX_HISTORY = 500          # messages kept per channel
SEND_HISTORY = 100         # messages sent when a channel is opened
MAX_TEXT = 1000
MAX_IMAGE_BYTES = 3 * 1024 * 1024
MAX_AVATAR_BYTES = 400 * 1024
MAX_VOICE_PACKET = 4096
MAX_SERVERS_PER_USER = 20
MAX_CHANNELS_PER_SERVER = 30


# ---------------------------------------------------------------- storage
def _pg():
    import psycopg  # only needed when DATABASE_URL is set

    return psycopg.connect(DATABASE_URL)


def storage_init():
    if DATABASE_URL:
        last = None
        for attempt in range(6):  # the free database may need a few seconds to wake up
            try:
                with _pg() as c:
                    c.execute(
                        "CREATE TABLE IF NOT EXISTS kv "
                        "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                    )
                    c.execute(
                        "CREATE TABLE IF NOT EXISTS images "
                        "(id TEXT PRIMARY KEY, data BYTEA NOT NULL)"
                    )
                return
            except Exception as e:
                last = e
                print(f"database not ready (try {attempt + 1}/6):", e)
                time.sleep(4)
        raise RuntimeError(f"Could not reach the database: {last}")
    os.makedirs(IMG_DIR, exist_ok=True)


def load_state():
    """Returns the saved data, or None if nothing was ever saved.
    If saved data exists but can't be read, this raises instead of returning
    None, so a bad start can never overwrite good data with an empty save."""
    if DATABASE_URL:
        last = None
        for attempt in range(6):
            try:
                with _pg() as c:
                    row = c.execute("SELECT value FROM kv WHERE key = 'state'").fetchone()
                return json.loads(row[0]) if row else None
            except Exception as e:
                last = e
                print(f"database read failed (try {attempt + 1}/6):", e)
                time.sleep(4)
        raise RuntimeError(f"Could not read from the database: {last}")
    path = os.path.join(DATA_DIR, "state.json")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            backup = f"{path}.broken-{int(time.time())}"
            os.replace(path, backup)
            print("state.json was unreadable, kept a copy at", backup, e)
    return None


def save_state(text):
    """Returns True if the save worked."""
    try:
        if DATABASE_URL:
            with _pg() as c:
                c.execute(
                    "INSERT INTO kv (key, value) VALUES ('state', %s) "
                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                    (text,),
                )
        else:
            os.makedirs(DATA_DIR, exist_ok=True)
            path = os.path.join(DATA_DIR, "state.json")
            with open(path + ".tmp", "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(path + ".tmp", path)
        return True
    except Exception as e:
        print("save_state failed:", e)
        return False


def put_image(image_id, data):
    if DATABASE_URL:
        with _pg() as c:
            c.execute(
                "INSERT INTO images (id, data) VALUES (%s, %s) "
                "ON CONFLICT (id) DO NOTHING",
                (image_id, data),
            )
    else:
        os.makedirs(IMG_DIR, exist_ok=True)
        with open(os.path.join(IMG_DIR, image_id + ".jpg"), "wb") as f:
            f.write(data)


def get_image(image_id):
    try:
        if DATABASE_URL:
            with _pg() as c:
                row = c.execute(
                    "SELECT data FROM images WHERE id = %s", (image_id,)
                ).fetchone()
            return bytes(row[0]) if row else None
        path = os.path.join(IMG_DIR, image_id + ".jpg")
        if os.path.exists(path):
            with open(path, "rb") as f:
                return f.read()
    except Exception as e:
        print("get_image failed:", e)
    return None


# ------------------------------------------------------------ in-memory data
db = {"users": {}, "servers": {}, "messages": {}, "tokens": {}}

storage_init()
_loaded = load_state()
if _loaded:
    for _k in db:
        db[_k] = _loaded.get(_k, db[_k])

if not DATABASE_URL:
    print("WARNING: no DATABASE_URL set - data is saved to local disk, which free "
          "hosts wipe on restart.")

online = {}          # name -> websocket
voice_members = {}   # channel id -> set of names
user_voice = {}      # name -> channel id they're in
login_fails = {}     # name -> (count, first time)

_dirty = False


def mark():
    global _dirty
    _dirty = True


SAVE_EVERY = 10  # seconds between saves (only when something changed)


async def saver():
    global _dirty
    while True:
        await asyncio.sleep(SAVE_EVERY)
        if _dirty:
            _dirty = False
            text = json.dumps(db)
            ok = await asyncio.to_thread(save_state, text)
            if not ok:
                _dirty = True  # try again next round


def _ping(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        r.read()


async def keepalive():
    """Optional: set KEEP_AWAKE=1 on Render and the server pokes its own public
    address every 10 minutes so the free plan doesn't put it to sleep."""
    url = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
    if not url or os.environ.get("KEEP_AWAKE", "").lower() not in ("1", "true", "yes"):
        return
    while True:
        await asyncio.sleep(600)
        try:
            await asyncio.to_thread(_ping, url)
        except Exception as e:
            print("keepalive ping failed:", e)


@asynccontextmanager
async def lifespan(_app):
    tasks = [asyncio.create_task(saver()), asyncio.create_task(keepalive())]
    yield
    for t in tasks:
        t.cancel()
    save_state(json.dumps(db))


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------- helpers
def sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def hash_pw(password, salt_hex):
    return hashlib.pbkdf2_hmac(
        "sha256", password.encode(), bytes.fromhex(salt_hex), 100_000
    ).hex()


def dm_id(a, b):
    return "dm:" + ":".join(sorted([a, b]))


def server_of_channel(cid):
    for s in db["servers"].values():
        for c in s["channels"]:
            if c["id"] == cid:
                return s
    return None


def channel_of(cid):
    s = server_of_channel(cid)
    if s:
        for c in s["channels"]:
            if c["id"] == cid:
                return c
    return None


def can_access(name, cid):
    if not isinstance(cid, str):
        return False
    if cid.startswith("dm:"):
        parts = cid.split(":")
        if len(parts) != 3 or name not in parts[1:]:
            return False
        other = parts[2] if parts[1] == name else parts[1]
        return other in db["users"].get(name, {}).get("friends", [])
    s = server_of_channel(cid)
    return s is not None and name in s["members"]


def recipients(cid):
    if cid.startswith("dm:"):
        return cid.split(":")[1:]
    s = server_of_channel(cid)
    return list(s["members"]) if s else []


def related(name):
    u = db["users"][name]
    people = set(u["friends"]) | set(u["incoming"]) | set(u["outgoing"])
    for sid in u["servers"]:
        s = db["servers"].get(sid)
        if s:
            people |= set(s["members"])
    people.discard(name)
    return people


def avatar_of(name):
    return db["users"].get(name, {}).get("avatar")


def build_state(name):
    u = db["users"][name]
    servers = []
    for sid in u["servers"]:
        s = db["servers"].get(sid)
        if not s:
            continue
        servers.append({
            "id": sid,
            "name": s["name"],
            "owner": s["owner"],
            "invite": s["invite"],
            "members": [
                {"name": m, "online": m in online, "avatar": avatar_of(m)}
                for m in sorted(s["members"])
            ],
            "channels": [
                {
                    "id": c["id"],
                    "name": c["name"],
                    "kind": c["kind"],
                    "voice": sorted(voice_members.get(c["id"], [])),
                }
                for c in s["channels"]
            ],
        })
    return {
        "type": "state",
        "me": name,
        "profile": {"avatar": u.get("avatar"), "bio": u.get("bio", "")},
        "friends": [
            {"name": f, "online": f in online, "avatar": avatar_of(f)}
            for f in sorted(u["friends"])
        ],
        "incoming": sorted(u["incoming"]),
        "outgoing": sorted(u["outgoing"]),
        "servers": servers,
    }


async def send(ws, obj):
    try:
        await ws.send_text(json.dumps(obj))
    except Exception:
        pass


async def err(ws, text, **extra):
    await send(ws, {"type": "error", "message": text, **extra})


async def push_state(name):
    ws = online.get(name)
    if ws is not None and name in db["users"]:
        await send(ws, build_state(name))


async def push_states(names):
    for n in list(names):
        await push_state(n)


async def broadcast(cid, obj, skip=None):
    for r in recipients(cid):
        target = online.get(r)
        if target is not None and r != skip:
            await send(target, obj)


async def leave_voice(name):
    cid = user_voice.pop(name, None)
    if cid is None:
        return
    members = voice_members.get(cid)
    if members:
        members.discard(name)
        if not members:
            voice_members.pop(cid, None)
    s = server_of_channel(cid)
    if s:
        await push_states(s["members"])


def new_channel(name, kind):
    return {"id": secrets.token_hex(4), "name": name, "kind": kind}


def unique_invite():
    while True:
        code = secrets.token_hex(3)
        if all(s["invite"] != code for s in db["servers"].values()):
            return code


def drop_channel_data(cid):
    db["messages"].pop(cid, None)
    voice_members.pop(cid, None)
    for n, c in list(user_voice.items()):
        if c == cid:
            user_voice.pop(n, None)


async def accept_friendship(a, b):
    ua, ub = db["users"][a], db["users"][b]
    for lst, other in ((ua["incoming"], b), (ua["outgoing"], b),
                       (ub["incoming"], a), (ub["outgoing"], a)):
        if other in lst:
            lst.remove(other)
    if b not in ua["friends"]:
        ua["friends"].append(b)
    if a not in ub["friends"]:
        ub["friends"].append(a)
    mark()
    await push_states([a, b])


# ------------------------------------------------------------- HTTP routes
@app.get("/")
async def root():
    return {
        "status": "Talky server is running",
        "version": "2.1",
        "storage": "database (saved permanently)" if DATABASE_URL
        else "local disk (WIPED whenever the server restarts)",
    }


@app.get("/img/{image_id}")
async def image(image_id: str):
    if not IMAGE_ID_RE.match(image_id):
        return Response(status_code=404)
    data = await asyncio.to_thread(get_image, image_id)
    if data is None:
        return Response(status_code=404)
    return Response(
        content=data,
        media_type="image/jpeg",
        headers={
            "Cache-Control": "public, max-age=31536000",
            "X-Content-Type-Options": "nosniff",
        },
    )


# ---------------------------------------------------------------- websocket
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    me = None
    recent = deque(maxlen=8)  # timestamps of recent sends, for rate limiting
    try:
        while True:
            packet = await ws.receive()
            if packet.get("type") == "websocket.disconnect":
                break

            # ---- voice audio (binary frames) ----
            data = packet.get("bytes")
            if data is not None:
                cid = user_voice.get(me) if me else None
                if cid and len(data) <= MAX_VOICE_PACKET:
                    who = me.encode()
                    frame = bytes([len(who)]) + who + data
                    for m in list(voice_members.get(cid, ())):
                        target = online.get(m)
                        if m != me and target is not None:
                            try:
                                await target.send_bytes(frame)
                            except Exception:
                                pass
                continue

            text = packet.get("text")
            if text is None:
                continue
            try:
                msg = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(msg, dict):
                continue
            kind = msg.get("type")

            # ================= log in / sign up =================
            if me is None:
                if kind != "login":
                    continue
                name = str(msg.get("name", "")).strip().lower()
                password = str(msg.get("password") or "")
                token = str(msg.get("token") or "")

                if not NAME_RE.match(name):
                    await err(ws, "Names must be 2-20 letters, numbers or underscores.")
                    await ws.close()
                    return

                count, first = login_fails.get(name, (0, time.time()))
                if time.time() - first > 60:
                    count, first = 0, time.time()
                if count >= 8:
                    await err(ws, "Too many wrong tries. Wait a minute and try again.")
                    await ws.close()
                    return

                user = db["users"].get(name)
                new_token = None
                if token:
                    if user and db["tokens"].get(sha(token)) == name:
                        new_token = token
                    else:
                        await err(ws, "Your saved sign-in expired. Please log in again.", relogin=True)
                        await ws.close()
                        return
                else:
                    if len(password) < 4:
                        await err(ws, "Password must be at least 4 characters.")
                        await ws.close()
                        return
                    if user is None:
                        salt = secrets.token_hex(16)
                        db["users"][name] = user = {
                            "salt": salt,
                            "hash": hash_pw(password, salt),
                            "friends": [],
                            "incoming": [],
                            "outgoing": [],
                            "servers": [],
                            "avatar": None,
                            "bio": "",
                            "created": int(time.time()),
                        }
                    elif not hmac.compare_digest(
                        user["hash"], hash_pw(password, user["salt"])
                    ):
                        login_fails[name] = (count + 1, first)
                        await err(ws, "Wrong password for that name.")
                        await ws.close()
                        return
                    new_token = secrets.token_urlsafe(24)
                    db["tokens"][sha(new_token)] = name
                    mine = [h for h, n in db["tokens"].items() if n == name]
                    for h in mine[:-5]:  # keep the 5 newest sign-ins
                        db["tokens"].pop(h, None)
                    mark()

                login_fails.pop(name, None)
                old = online.get(name)
                if old is not None and old is not ws:
                    try:
                        await err(old, "You signed in somewhere else, so this window was signed out.", kicked=True)
                        await old.close()
                    except Exception:
                        pass
                me = name
                online[me] = ws
                await send(ws, {"type": "login_ok", "name": me, "token": new_token})
                await push_state(me)
                await push_states(related(me))
                continue

            u = db["users"][me]

            if kind == "ping":
                await send(ws, {"type": "pong"})
                continue

            # ================= profile =================
            if kind == "set_avatar":
                data = msg.get("data")
                if data is None:
                    u["avatar"] = None
                else:
                    try:
                        raw = base64.b64decode(str(data), validate=True)
                    except (binascii.Error, ValueError):
                        await err(ws, "That picture couldn't be read.")
                        continue
                    if len(raw) > MAX_AVATAR_BYTES or not raw.startswith(b"\xff\xd8\xff"):
                        await err(ws, "That picture isn't usable (JPEG up to 400 KB).")
                        continue
                    image_id = secrets.token_hex(8)
                    try:
                        await asyncio.to_thread(put_image, image_id, raw)
                    except Exception as e:
                        print("put_image failed:", e)
                        await err(ws, "The server couldn't save that picture.")
                        continue
                    u["avatar"] = image_id
                mark()
                await push_states(related(me) | {me})

            elif kind == "set_bio":
                u["bio"] = str(msg.get("bio", "")).strip()[:120]
                mark()
                await push_state(me)

            elif kind == "get_profile":
                target = str(msg.get("name", "")).strip().lower()
                tu = db["users"].get(target)
                if tu is None or (target != me and target not in related(me)):
                    await err(ws, "Couldn't find that profile.")
                else:
                    await send(ws, {
                        "type": "profile",
                        "name": target,
                        "avatar": tu.get("avatar"),
                        "bio": tu.get("bio", ""),
                        "created": tu.get("created"),
                        "online": target in online,
                    })

            # ================= friends =================
            elif kind == "add_friend":
                target = str(msg.get("name", "")).strip().lower()
                other = db["users"].get(target)
                if target == me:
                    await err(ws, "That's you!")
                elif other is None:
                    await err(ws, f'No user called "{target}". They need to sign up first.')
                elif target in u["friends"]:
                    await err(ws, f"You're already friends with {target}.")
                elif target in u["outgoing"]:
                    await err(ws, f"You already sent {target} a request.")
                elif target in u["incoming"]:
                    await accept_friendship(me, target)
                else:
                    u["outgoing"].append(target)
                    other["incoming"].append(me)
                    mark()
                    await push_states([me, target])

            elif kind == "respond_friend":
                target = str(msg.get("name", "")).strip().lower()
                action = msg.get("action")
                other = db["users"].get(target)
                if other is None:
                    continue
                if action == "accept" and target in u["incoming"]:
                    await accept_friendship(me, target)
                elif action == "decline" and target in u["incoming"]:
                    u["incoming"].remove(target)
                    if me in other["outgoing"]:
                        other["outgoing"].remove(me)
                    mark()
                    await push_states([me, target])
                elif action == "cancel" and target in u["outgoing"]:
                    u["outgoing"].remove(target)
                    if me in other["incoming"]:
                        other["incoming"].remove(me)
                    mark()
                    await push_states([me, target])

            elif kind == "remove_friend":
                target = str(msg.get("name", "")).strip().lower()
                other = db["users"].get(target)
                if other is not None and target in u["friends"]:
                    u["friends"].remove(target)
                    if me in other["friends"]:
                        other["friends"].remove(me)
                    mark()
                    await push_states([me, target])

            # ================= servers =================
            elif kind == "create_server":
                name = str(msg.get("name", "")).strip()[:30]
                if not name:
                    await err(ws, "Give your server a name.")
                elif len(u["servers"]) >= MAX_SERVERS_PER_USER:
                    await err(ws, "You're in too many servers already.")
                else:
                    sid = secrets.token_hex(4)
                    db["servers"][sid] = {
                        "id": sid,
                        "name": name,
                        "owner": me,
                        "invite": unique_invite(),
                        "members": [me],
                        "channels": [
                            new_channel("general", "text"),
                            new_channel("Lounge", "voice"),
                        ],
                    }
                    u["servers"].append(sid)
                    mark()
                    await push_state(me)
                    await send(ws, {"type": "focus", "server": sid})

            elif kind == "join_server":
                code = str(msg.get("code", "")).strip().lower()
                target = next(
                    (s for s in db["servers"].values() if s["invite"] == code), None
                )
                if target is None:
                    await err(ws, "That invite code doesn't match any server.")
                elif me in target["members"]:
                    await err(ws, "You're already in that server.")
                elif len(u["servers"]) >= MAX_SERVERS_PER_USER:
                    await err(ws, "You're in too many servers already.")
                else:
                    target["members"].append(me)
                    u["servers"].append(target["id"])
                    mark()
                    await push_states(target["members"])
                    await send(ws, {"type": "focus", "server": target["id"]})

            elif kind == "leave_server":
                s = db["servers"].get(msg.get("sid"))
                if s is None or me not in s["members"]:
                    continue
                if s["owner"] == me:
                    await err(ws, "Owners can't leave. Delete the server instead.")
                    continue
                if user_voice.get(me) in [c["id"] for c in s["channels"]]:
                    await leave_voice(me)
                s["members"].remove(me)
                if s["id"] in u["servers"]:
                    u["servers"].remove(s["id"])
                mark()
                await push_states(set(s["members"]) | {me})

            elif kind == "delete_server":
                s = db["servers"].get(msg.get("sid"))
                if s is None or s["owner"] != me:
                    await err(ws, "Only the owner can delete a server.")
                    continue
                members = list(s["members"])
                for c in s["channels"]:
                    drop_channel_data(c["id"])
                for m in members:
                    mu = db["users"].get(m)
                    if mu and s["id"] in mu["servers"]:
                        mu["servers"].remove(s["id"])
                db["servers"].pop(s["id"], None)
                mark()
                await push_states(members)

            elif kind == "rename_server":
                s = db["servers"].get(msg.get("sid"))
                name = str(msg.get("name", "")).strip()[:30]
                if s is None or s["owner"] != me:
                    await err(ws, "Only the owner can rename the server.")
                elif name:
                    s["name"] = name
                    mark()
                    await push_states(s["members"])

            elif kind == "kick":
                s = db["servers"].get(msg.get("sid"))
                target = str(msg.get("name", "")).strip().lower()
                if s is None or s["owner"] != me:
                    await err(ws, "Only the owner can remove people.")
                elif target == me or target not in s["members"]:
                    await err(ws, "You can't remove that person.")
                else:
                    if user_voice.get(target) in [c["id"] for c in s["channels"]]:
                        await leave_voice(target)
                    s["members"].remove(target)
                    tu = db["users"].get(target)
                    if tu and s["id"] in tu["servers"]:
                        tu["servers"].remove(s["id"])
                    mark()
                    tw = online.get(target)
                    if tw is not None:
                        await send(tw, {"type": "info",
                                        "message": f"You were removed from {s['name']}."})
                    await push_states(set(s["members"]) | {target})

            elif kind == "create_channel":
                s = db["servers"].get(msg.get("sid"))
                name = str(msg.get("name", "")).strip().lower().replace(" ", "-")
                ckind = "voice" if msg.get("kind") == "voice" else "text"
                if s is None or s["owner"] != me:
                    await err(ws, "Only the server owner can make channels.")
                elif not CHANNEL_RE.match(name):
                    await err(ws, "Channel names: 1-24 letters, numbers or dashes.")
                elif len(s["channels"]) >= MAX_CHANNELS_PER_SERVER:
                    await err(ws, "This server has too many channels.")
                else:
                    s["channels"].append(new_channel(name, ckind))
                    mark()
                    await push_states(s["members"])

            elif kind == "delete_channel":
                s = db["servers"].get(msg.get("sid"))
                cid = msg.get("cid")
                if s is None or s["owner"] != me:
                    await err(ws, "Only the server owner can delete channels.")
                elif len(s["channels"]) <= 1:
                    await err(ws, "A server needs at least one channel.")
                elif any(c["id"] == cid for c in s["channels"]):
                    s["channels"] = [c for c in s["channels"] if c["id"] != cid]
                    drop_channel_data(cid)
                    mark()
                    await push_states(s["members"])

            # ================= messages =================
            elif kind == "history":
                cid = msg.get("channel")
                if can_access(me, cid):
                    await send(ws, {
                        "type": "history",
                        "channel": cid,
                        "messages": db["messages"].get(cid, [])[-SEND_HISTORY:],
                    })

            elif kind == "send":
                cid = msg.get("channel")
                if not can_access(me, cid):
                    continue
                now = time.time()
                if len(recent) == recent.maxlen and now - recent[0] < 5:
                    await err(ws, "Slow down a little!")
                    continue
                recent.append(now)

                mkind = msg.get("kind", "text")
                message = {
                    "id": secrets.token_hex(4),
                    "sender": me,
                    "time": int(now),
                    "kind": "text",
                    "text": "",
                }
                if mkind == "text":
                    body = str(msg.get("text", "")).strip()[:MAX_TEXT]
                    if not body:
                        continue
                    message["text"] = body
                elif mkind == "sticker":
                    body = str(msg.get("text", "")).strip()
                    if not body or len(body) > 8:
                        continue
                    message["kind"] = "sticker"
                    message["text"] = body
                elif mkind == "image":
                    try:
                        raw = base64.b64decode(str(msg.get("data", "")), validate=True)
                    except (binascii.Error, ValueError):
                        await err(ws, "That image couldn't be read.")
                        continue
                    if len(raw) > MAX_IMAGE_BYTES:
                        await err(ws, "That image is too big (limit 3 MB).")
                        continue
                    if not raw.startswith(b"\xff\xd8\xff"):
                        await err(ws, "Only JPEG images are accepted (the app converts them for you).")
                        continue
                    image_id = secrets.token_hex(8)
                    try:
                        await asyncio.to_thread(put_image, image_id, raw)
                    except Exception as e:
                        print("put_image failed:", e)
                        await err(ws, "The server couldn't save that image.")
                        continue
                    message["kind"] = "image"
                    message["image"] = image_id
                else:
                    continue

                history = db["messages"].setdefault(cid, [])
                history.append(message)
                del history[:-MAX_HISTORY]
                mark()
                for r in recipients(cid):
                    target = online.get(r)
                    if target is not None:
                        await send(target, {
                            "type": "message",
                            "channel": cid,
                            "message": message,
                        })

            elif kind == "edit_message":
                cid = msg.get("channel")
                if not can_access(me, cid):
                    continue
                found = next((m for m in db["messages"].get(cid, [])
                              if m["id"] == msg.get("id")), None)
                body = str(msg.get("text", "")).strip()[:MAX_TEXT]
                if found is None or found["sender"] != me or found.get("kind") != "text":
                    continue
                if not body:
                    await err(ws, "A message can't be empty. Delete it instead.")
                    continue
                found["text"] = body
                found["edited"] = True
                mark()
                await broadcast(cid, {"type": "message_updated", "channel": cid, "message": found})

            elif kind == "react":
                cid = msg.get("channel")
                emoji = str(msg.get("emoji", "")).strip()
                if not can_access(me, cid) or not emoji or len(emoji) > 8:
                    continue
                found = next((m for m in db["messages"].get(cid, [])
                              if m["id"] == msg.get("id")), None)
                if found is None:
                    continue
                reactions = found.setdefault("reactions", {})
                who = reactions.setdefault(emoji, [])
                if me in who:
                    who.remove(me)
                elif len(reactions) > 12:
                    reactions.pop(emoji, None)
                    await err(ws, "That message has too many different reactions.")
                    continue
                else:
                    who.append(me)
                if not who:
                    reactions.pop(emoji, None)
                if not reactions:
                    found.pop("reactions", None)
                mark()
                await broadcast(cid, {"type": "message_updated", "channel": cid, "message": found})

            elif kind == "typing":
                cid = msg.get("channel")
                if can_access(me, cid):
                    await broadcast(cid, {"type": "typing", "channel": cid, "name": me}, skip=me)

            elif kind == "delete_message":
                cid = msg.get("channel")
                mid = msg.get("id")
                if not can_access(me, cid):
                    continue
                history = db["messages"].get(cid, [])
                found = next((m for m in history if m["id"] == mid), None)
                if found is None:
                    continue
                s = server_of_channel(cid)
                if found["sender"] == me or (s is not None and s["owner"] == me):
                    history.remove(found)
                    mark()
                    for r in recipients(cid):
                        target = online.get(r)
                        if target is not None:
                            await send(target, {
                                "type": "message_deleted",
                                "channel": cid,
                                "id": mid,
                            })

            # ================= voice =================
            elif kind == "voice_join":
                cid = msg.get("channel")
                c = channel_of(cid)
                s = server_of_channel(cid)
                if c is None or c["kind"] != "voice" or me not in s["members"]:
                    await err(ws, "You can't join that voice channel.")
                    continue
                await leave_voice(me)
                voice_members.setdefault(cid, set()).add(me)
                user_voice[me] = cid
                await push_states(s["members"])

            elif kind == "voice_leave":
                await leave_voice(me)

    except Exception as e:
        print("connection error:", repr(e))
    finally:
        if me and online.get(me) is ws:
            del online[me]
            await leave_voice(me)
            if me in db["users"]:
                await push_states(related(me))
