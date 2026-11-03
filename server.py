"""Talky server: keeps track of users, friends and group chats, and passes
messages between everyone who is online. Run with:

    uvicorn server:app --host 0.0.0.0 --port 8000
"""
import hashlib
import hmac
import json
import os
import re
import secrets
import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

DATA_FILE = os.environ.get("DATA_FILE", "data.json")
NAME_RE = re.compile(r"^[a-z0-9_]{2,20}$")
MAX_HISTORY = 200
MAX_MESSAGE_LENGTH = 1000

users = {}   # name -> {"salt": str, "hash": str, "friends": [names]}
chats = {}   # id -> {"id": str, "name": str, "members": [names], "messages": [...]}
online = {}  # name -> websocket


def load():
    global users, chats
    try:
        with open(DATA_FILE) as f:
            data = json.load(f)
        users = data.get("users", {})
        chats = data.get("chats", {})
    except Exception:
        pass


def save():
    try:
        with open(DATA_FILE, "w") as f:
            json.dump({"users": users, "chats": chats}, f)
    except Exception:
        pass


load()


def hash_pw(password, salt_hex):
    return hashlib.pbkdf2_hmac(
        "sha256", password.encode(), bytes.fromhex(salt_hex), 100_000
    ).hex()


async def send(ws, obj):
    try:
        await ws.send_text(json.dumps(obj))
    except Exception:
        pass


async def err(ws, text):
    await send(ws, {"type": "error", "message": text})


def friend_list(name):
    return [
        {"name": f, "online": f in online}
        for f in sorted(users[name]["friends"])
    ]


def chat_view(chat):
    return {
        "id": chat["id"],
        "name": chat["name"],
        "members": chat["members"],
        "messages": chat["messages"][-MAX_HISTORY:],
    }


async def push_friends(name):
    ws = online.get(name)
    if ws is not None:
        await send(ws, {"type": "friends", "friends": friend_list(name)})


async def push_chat(chat):
    for member in chat["members"]:
        ws = online.get(member)
        if ws is not None:
            await send(ws, {"type": "chat", "chat": chat_view(chat)})


@app.get("/")
async def root():
    return {"status": "Talky server is running"}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    me = None
    try:
        while True:
            try:
                msg = json.loads(await ws.receive_text())
            except json.JSONDecodeError:
                continue
            if not isinstance(msg, dict):
                continue
            kind = msg.get("type")

            # ---------- log in / sign up ----------
            if me is None:
                if kind != "login":
                    continue
                name = str(msg.get("name", "")).strip().lower()
                password = str(msg.get("password", ""))
                if not NAME_RE.match(name):
                    await err(ws, "Names must be 2-20 letters, numbers or underscores.")
                    await ws.close()
                    return
                if len(password) < 4:
                    await err(ws, "Password must be at least 4 characters.")
                    await ws.close()
                    return
                user = users.get(name)
                if user is None:
                    salt = secrets.token_hex(16)
                    users[name] = {
                        "salt": salt,
                        "hash": hash_pw(password, salt),
                        "friends": [],
                    }
                    save()
                elif not hmac.compare_digest(
                    user["hash"], hash_pw(password, user["salt"])
                ):
                    await err(ws, "Wrong password for that name.")
                    await ws.close()
                    return

                old = online.get(name)
                if old is not None and old is not ws:
                    try:
                        await old.close()
                    except Exception:
                        pass
                me = name
                online[me] = ws
                await send(ws, {
                    "type": "login_ok",
                    "name": me,
                    "friends": friend_list(me),
                    "chats": [
                        chat_view(c) for c in chats.values() if me in c["members"]
                    ],
                })
                for f in users[me]["friends"]:
                    await push_friends(f)
                continue

            # ---------- add a friend ----------
            if kind == "add_friend":
                target = str(msg.get("name", "")).strip().lower()
                if target == me:
                    await err(ws, "That's you!")
                elif target not in users:
                    await err(ws, f'No user called "{target}". They need to open the app and sign up first.')
                elif target in users[me]["friends"]:
                    await err(ws, f"You're already friends with {target}.")
                else:
                    users[me]["friends"].append(target)
                    users[target]["friends"].append(me)
                    save()
                    await push_friends(me)
                    await push_friends(target)

            # ---------- create a group chat ----------
            elif kind == "create_chat":
                chat_name = str(msg.get("name", "")).strip()[:30]
                raw = msg.get("members")
                if not isinstance(raw, list):
                    raw = []
                members = [
                    m for m in dict.fromkeys(str(x).lower() for x in raw)
                    if m in users[me]["friends"]
                ]
                if not chat_name or not members:
                    await err(ws, "Give the chat a name and pick at least one friend.")
                else:
                    chat = {
                        "id": secrets.token_hex(4),
                        "name": chat_name,
                        "members": [me] + members,
                        "messages": [],
                    }
                    chats[chat["id"]] = chat
                    save()
                    await push_chat(chat)

            # ---------- add someone to an existing chat ----------
            elif kind == "add_member":
                chat = chats.get(msg.get("chat_id"))
                target = str(msg.get("name", "")).strip().lower()
                if chat is None or me not in chat["members"]:
                    await err(ws, "You're not in that chat.")
                elif target not in users[me]["friends"]:
                    await err(ws, f"{target} isn't one of your friends yet.")
                elif target in chat["members"]:
                    await err(ws, f"{target} is already in this chat.")
                else:
                    chat["members"].append(target)
                    save()
                    await push_chat(chat)

            # ---------- send a message ----------
            elif kind == "send":
                chat = chats.get(msg.get("chat_id"))
                text = str(msg.get("text", "")).strip()[:MAX_MESSAGE_LENGTH]
                if chat is None or me not in chat["members"] or not text:
                    continue
                message = {"sender": me, "text": text, "time": int(time.time())}
                chat["messages"].append(message)
                del chat["messages"][:-MAX_HISTORY]
                save()
                for member in chat["members"]:
                    member_ws = online.get(member)
                    if member_ws is not None:
                        await send(member_ws, {
                            "type": "message",
                            "chat_id": chat["id"],
                            "message": message,
                        })
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if me and online.get(me) is ws:
            del online[me]
            for f in users[me]["friends"]:
                await push_friends(f)
