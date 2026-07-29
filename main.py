"""
PixelGame – one-file backend
FastAPI + MongoDB + WebSockets
Canvas 4096x4096 | Clans | Lookup | Mod | Earn | Chat
"""

import asyncio
import time
import random
import secrets
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, List
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from passlib.context import CryptContext
from jose import jwt, JWTError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId

# ===================== CONFIG =====================
MONGO_URI = "mongodb+srv://user:pass@cluster.mongodb.net/pixelgame?retryWrites=true&w=majority"
MONGO_DB = "pixelgame"
SECRET_KEY = "change-this-to-a-very-long-random-secret-key-in-production-2026"
ALGORITHM = "HS256"
TOKEN_EXPIRE_MIN = 60 * 24 * 7

CANVAS_W = 4096
CANVAS_H = 4096
CHUNK = 256

GUEST_MAX = 60
AUTH_MAX = 200
DISCORD_BONUS = 100
MOD_BONUS = 15
CLAN_MEMBER_BONUS = 5
CLAN_MAX = 20

EARN_REWARD = 15
EARN_OK_CD = 7.0
EARN_FAIL_CD = 5.0
MOD_CODE = "237360049320122092250232257"

# ===================== PALETTE (50) =====================
PALETTE = [
    "#000000", "#FFFFFF", "#C0C0C0", "#808080", "#404040",
    "#FF0000", "#FF4000", "#FF8000", "#FFBF00", "#FFFF00",
    "#BFFF00", "#80FF00", "#40FF00", "#00FF00", "#00FF40",
    "#00FF80", "#00FFBF", "#00FFFF", "#00BFFF", "#0080FF",
    "#0040FF", "#0000FF", "#4000FF", "#8000FF", "#BF00FF",
    "#FF00FF", "#FF00BF", "#FF0080", "#FF0040", "#800000",
    "#804000", "#808000", "#408000", "#008000", "#008040",
    "#008080", "#004080", "#000080", "#400080", "#800080",
    "#FF6666", "#FFB366", "#FFFF66", "#B3FF66", "#66FFB3",
    "#66B3FF", "#B366FF", "#FF66B3", "#A0522D", "#D2691E",
]

# ===================== ANIMALS =====================
ANIMALS = [
    {"id": "cat", "name": "Cat", "emoji": "🐱"},
    {"id": "dog", "name": "Dog", "emoji": "🐶"},
    {"id": "tiger", "name": "Tiger", "emoji": "🐯"},
    {"id": "bear", "name": "Bear", "emoji": "🐻"},
    {"id": "antelope", "name": "Antelope", "emoji": "🦌"},
    {"id": "elephant", "name": "Elephant", "emoji": "🐘"},
    {"id": "hippo", "name": "Hippo", "emoji": "🦛"},
    {"id": "crocodile", "name": "Crocodile", "emoji": "🐊"},
    {"id": "human", "name": "Human", "emoji": "🧑"},
    {"id": "mosquito", "name": "Mosquito", "emoji": "🦟"},
    {"id": "pig", "name": "Pig", "emoji": "🐷"},
    {"id": "fox", "name": "Fox", "emoji": "🦊"},
    {"id": "wolf", "name": "Wolf", "emoji": "🐺"},
    {"id": "rabbit", "name": "Rabbit", "emoji": "🐰"},
    {"id": "owl", "name": "Owl", "emoji": "🦉"},
    {"id": "frog", "name": "Frog", "emoji": "🐸"},
    {"id": "snake", "name": "Snake", "emoji": "🐍"},
    {"id": "lion", "name": "Lion", "emoji": "🦁"},
    {"id": "panda", "name": "Panda", "emoji": "🐼"},
    {"id": "koala", "name": "Koala", "emoji": "🐨"},
    {"id": "monkey", "name": "Monkey", "emoji": "🐵"},
]

SVG = {
    "cat": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" width="120" height="120"><circle cx="32" cy="34" r="18" fill="#FFB347"/><polygon points="14,20 22,8 26,22" fill="#FFB347"/><polygon points="50,20 42,8 38,22" fill="#FFB347"/><circle cx="24" cy="32" r="3" fill="#222"/><circle cx="40" cy="32" r="3" fill="#222"/><path d="M26 42 Q32 48 38 42" stroke="#222" stroke-width="2" fill="none"/></svg>',
    "dog": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" width="120" height="120"><ellipse cx="32" cy="36" rx="18" ry="16" fill="#D2A679"/><ellipse cx="14" cy="28" rx="6" ry="10" fill="#D2A679"/><ellipse cx="50" cy="28" rx="6" ry="10" fill="#D2A679"/><circle cx="26" cy="34" r="3" fill="#222"/><circle cx="38" cy="34" r="3" fill="#222"/><ellipse cx="32" cy="42" rx="4" ry="3" fill="#222"/></svg>',
    "default": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" width="120" height="120"><circle cx="32" cy="32" r="20" fill="#88CC88"/><text x="32" y="38" text-anchor="middle" font-size="28">?</text></svg>',
}

# ===================== APP =====================
pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")
app = FastAPI(title="PixelGame")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

client = None
db = None
canvas_chunks: Dict[str, Dict[str, int]] = {}
ws_clients: Set[WebSocket] = set()
guest_challenges: Dict = {}
current_event = {"active": False, "options": ["red", "blue", "green", "orange"], "votes": {"red": 0, "blue": 0, "green": 0, "orange": 0}, "winner": None}
scheduler = AsyncIOScheduler()


def hash_pw(p): return pwd.hash(p)
def verify_pw(p, h): return pwd.verify(p, h)
def make_token(u):
    return jwt.encode({"sub": u, "exp": datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_MIN)}, SECRET_KEY, algorithm=ALGORITHM)
def decode_token(t):
    try:
        return jwt.decode(t, SECRET_KEY, algorithms=[ALGORITHM]).get("sub")
    except JWTError:
        return None
def ck(x, y): return f"{x // CHUNK}_{y // CHUNK}"
def pk(x, y): return f"{x},{y}"


async def calc_max(user):
    base = AUTH_MAX if user.get("password_hash") else GUEST_MAX
    bonus = 0
    if user.get("is_mod"): bonus += MOD_BONUS
    if user.get("has_discord"): bonus += DISCORD_BONUS
    if user.get("clan_id"):
        clan = await db.clans.find_one({"_id": user["clan_id"]})
        if clan:
            bonus += min(len(clan.get("members", [])), CLAN_MAX) * CLAN_MEMBER_BONUS
    return base + bonus


async def broadcast(data, exclude=None):
    dead = []
    for ws in list(ws_clients):
        if ws is exclude: continue
        try:
            await ws.send_json(data)
        except Exception:
            dead.append(ws)
    for d in dead:
        ws_clients.discard(d)


@app.on_event("startup")
async def startup():
    global client, db
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[MONGO_DB]
    await db.users.create_index("username", unique=True)
    await db.pixels.create_index([("x", 1), ("y", 1)], unique=True)
    await db.clans.create_index("name", unique=True)
    await db.chat.create_index([("channel", 1), ("ts", -1)])
    scheduler.add_job(run_color_event, "interval", hours=2, id="ev")
    scheduler.start()
    print("[OK] PixelGame started")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()
    if client: client.close()


# ---------- AUTH ----------
class Reg(BaseModel):
    username: str = Field(..., min_length=3, max_length=24)
    password: str = Field(..., min_length=4, max_length=64)

class Login(BaseModel):
    username: str
    password: str


@app.post("/api/register")
async def register(d: Reg):
    if await db.users.find_one({"username": d.username.lower()}):
        raise HTTPException(400, "Username taken")
    await db.users.insert_one({
        "username": d.username.lower(), "display_name": d.username,
        "password_hash": hash_pw(d.password), "pixels": 0,
        "is_mod": False, "has_discord": False, "clan_id": None,
        "total_placed": 0, "language": "en", "last_earn": 0.0,
        "muted_until": 0.0, "place_ban_until": 0.0, "banned": False,
        "created_at": datetime.utcnow(),
    })
    return {"token": make_token(d.username.lower()), "username": d.username}


@app.post("/api/login")
async def login(d: Login):
    u = await db.users.find_one({"username": d.username.lower()})
    if not u or not verify_pw(d.password, u["password_hash"]):
        raise HTTPException(401, "Invalid credentials")
    if u.get("banned"):
        raise HTTPException(403, "Banned")
    mx = await calc_max(u)
    return {
        "token": make_token(u["username"]),
        "username": u.get("display_name", u["username"]),
        "pixels": u.get("pixels", 0), "max_pixels": mx,
        "is_mod": u.get("is_mod", False), "has_discord": u.get("has_discord", False),
        "language": u.get("language", "en"),
    }


@app.get("/api/me")
async def me(authorization: Optional[str] = None):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "No token")
    un = decode_token(authorization[7:])
    if not un: raise HTTPException(401, "Invalid token")
    u = await db.users.find_one({"username": un})
    if not u: raise HTTPException(404, "Not found")
    mx = await calc_max(u)
    medals = []
    if u.get("has_discord"): medals.append("discord")
    if u.get("is_mod"): medals.append("mod")
    if u.get("clan_id"): medals.append("clan")
    return {
        "username": u.get("display_name", u["username"]),
        "pixels": u.get("pixels", 0), "max_pixels": mx,
        "is_mod": u.get("is_mod", False), "has_discord": u.get("has_discord", False),
        "clan_id": str(u["clan_id"]) if u.get("clan_id") else None,
        "medals": medals, "total_placed": u.get("total_placed", 0),
        "language": u.get("language", "en"),
    }


# ---------- CANVAS ----------
@app.get("/api/palette")
async def get_palette():
    return {"palette": PALETTE}


@app.get("/api/chunk/{cx}/{cy}")
async def get_chunk(cx: int, cy: int):
    key = f"{cx}_{cy}"
    if key in canvas_chunks:
        return {"chunk": key, "pixels": [[int(k.split(",")[0]), int(k.split(",")[1]), v] for k, v in canvas_chunks[key].items()]}
    x0, y0 = cx * CHUNK, cy * CHUNK
    pixels, chunk_data = [], {}
    async for p in db.pixels.find({"x": {"$gte": x0, "$lt": x0 + CHUNK}, "y": {"$gte": y0, "$lt": y0 + CHUNK}}):
        pixels.append([p["x"], p["y"], p["c"]])
        chunk_data[pk(p["x"], p["y"])] = p["c"]
    canvas_chunks[key] = chunk_data
    return {"chunk": key, "pixels": pixels}


class PlaceReq(BaseModel):
    x: int
    y: int
    color: int
    token: Optional[str] = None


@app.post("/api/place")
async def place(req: PlaceReq):
    if not (0 <= req.x < CANVAS_W and 0 <= req.y < CANVAS_H):
        raise HTTPException(400, "Out of bounds")
    if not (1 <= req.color < len(PALETTE)):
        raise HTTPException(400, "Invalid color")
    user = username = None
    if req.token:
        username = decode_token(req.token)
        if username:
            user = await db.users.find_one({"username": username})
            if user:
                if user.get("banned"):
                    await db.users.update_one({"username": username}, {"$inc": {"pixels": -30}})
                    raise HTTPException(403, "Banned. Penalty -30")
                if user.get("place_ban_until", 0) > time.time():
                    left = int(user["place_ban_until"] - time.time())
                    raise HTTPException(403, f"Place restricted {left}s")
    new_pixels = None
    if user:
        if user.get("pixels", 0) <= 0:
            raise HTTPException(400, "No pixels left")
        await db.users.update_one({"username": username}, {"$inc": {"pixels": -1, "total_placed": 1}})
        new_pixels = user.get("pixels", 0) - 1
    key, pkey = ck(req.x, req.y), pk(req.x, req.y)
    if key not in canvas_chunks: canvas_chunks[key] = {}
    canvas_chunks[key][pkey] = req.color
    await db.pixels.update_one({"x": req.x, "y": req.y}, {"$set": {"x": req.x, "y": req.y, "c": req.color, "u": username, "t": time.time()}}, upsert=True)
    await broadcast({"type": "pixel", "x": req.x, "y": req.y, "c": req.color, "u": username})
    return {"ok": True, "pixels_left": new_pixels}


# ---------- LOOKUP ----------
@app.get("/api/pixel/{x}/{y}")
async def lookup(x: int, y: int):
    pix = await db.pixels.find_one({"x": x, "y": y})
    if not pix:
        return {"x": x, "y": y, "empty": True}
    owner = None
    if pix.get("u"):
        u = await db.users.find_one({"username": pix["u"]})
        if u:
            medals = []
            clan_name = None
            if u.get("has_discord"): medals.append("discord")
            if u.get("is_mod"): medals.append("mod")
            if u.get("clan_id"):
                medals.append("clan")
                clan = await db.clans.find_one({"_id": u["clan_id"]})
                clan_name = clan["name"] if clan else None
            owner = {
                "username": u.get("display_name", u["username"]),
                "username_id": u["username"],
                "is_mod": u.get("is_mod", False),
                "medals": medals, "clan_name": clan_name,
                "total_placed": u.get("total_placed", 0),
                "banned": u.get("banned", False),
            }
    return {"x": x, "y": y, "color": pix.get("c"), "time": pix.get("t"), "owner": owner, "empty": False}


# ---------- MOD ----------
class ModAct(BaseModel):
    target: str
    token: str
    action: str
    minutes: Optional[int] = 0


@app.post("/api/mod/action")
async def mod_action(d: ModAct):
    mod_name = decode_token(d.token)
    if not mod_name: raise HTTPException(401, "Invalid token")
    mod = await db.users.find_one({"username": mod_name})
    if not mod or not mod.get("is_mod"): raise HTTPException(403, "Not a mod")
    target = await db.users.find_one({"username": d.target.lower()})
    if not target: raise HTTPException(404, "User not found")
    if target.get("is_mod") and target["username"] != mod_name:
        raise HTTPException(403, "Cannot moderate another mod")
    now = time.time()
    t = d.target.lower()
    if d.action == "ban":
        await db.users.update_one({"username": t}, {"$set": {"banned": True}})
        return {"ok": True, "message": f"{d.target} banned"}
    elif d.action == "unban":
        await db.users.update_one({"username": t}, {"$set": {"banned": False}})
        return {"ok": True, "message": f"{d.target} unbanned"}
    elif d.action == "delete":
        if target.get("clan_id"):
            await db.clans.update_one({"_id": target["clan_id"]}, {"$pull": {"members": t}})
        await db.users.delete_one({"username": t})
        return {"ok": True, "message": f"Account {d.target} deleted"}
    elif d.action == "mute":
        mins = max(1, min(d.minutes or 10, 2880))
        await db.users.update_one({"username": t}, {"$set": {"muted_until": now + mins * 60}})
        return {"ok": True, "message": f"Muted {mins} min"}
    elif d.action == "place_ban":
        mins = max(1, min(d.minutes or 10, 2880))
        await db.users.update_one({"username": t}, {"$set": {"place_ban_until": now + mins * 60}})
        return {"ok": True, "message": f"Place banned {mins} min"}
    raise HTTPException(400, "Unknown action")


class ModCodeReq(BaseModel):
    code: str
    token: str


@app.post("/api/mod/activate")
async def activate_mod(d: ModCodeReq):
    un = decode_token(d.token)
    if not un: raise HTTPException(401, "Invalid token")
    if d.code.strip() != MOD_CODE: raise HTTPException(400, "Wrong code")
    await db.users.update_one({"username": un}, {"$set": {"is_mod": True}})
    return {"ok": True, "message": "You are now a moderator"}


# ---------- CLANS ----------
class ClanCreate(BaseModel):
    name: str
    token: str

class ClanJoin(BaseModel):
    clan_id: str
    token: str

class ClanLeave(BaseModel):
    token: str


@app.get("/api/clans")
async def list_clans():
    clans = []
    async for c in db.clans.find().sort("name", 1):
        clans.append({
            "id": str(c["_id"]), "name": c["name"], "leader": c.get("leader"),
            "members": c.get("members", []), "member_count": len(c.get("members", [])),
            "max_members": CLAN_MAX,
        })
    return {"clans": clans}


@app.post("/api/clans/create")
async def create_clan(d: ClanCreate):
    un = decode_token(d.token)
    if not un: raise HTTPException(401, "Login required")
    name = d.name.strip()[:24]
    if len(name) < 2: raise HTTPException(400, "Name too short")
    u = await db.users.find_one({"username": un})
    if not u: raise HTTPException(404, "User not found")
    if u.get("clan_id"): raise HTTPException(400, "Already in a clan")
    if await db.clans.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}}):
        raise HTTPException(400, "Name taken")
    res = await db.clans.insert_one({"name": name, "leader": un, "members": [un], "created_at": datetime.utcnow()})
    await db.users.update_one({"username": un}, {"$set": {"clan_id": res.inserted_id}})
    return {"ok": True, "clan_id": str(res.inserted_id), "name": name}


@app.post("/api/clans/join")
async def join_clan(d: ClanJoin):
    un = decode_token(d.token)
    if not un: raise HTTPException(401, "Login required")
    u = await db.users.find_one({"username": un})
    if not u: raise HTTPException(404, "User not found")
    if u.get("clan_id"): raise HTTPException(400, "Already in a clan")
    try: oid = ObjectId(d.clan_id)
    except Exception: raise HTTPException(400, "Invalid id")
    clan = await db.clans.find_one({"_id": oid})
    if not clan: raise HTTPException(404, "Clan not found")
    if len(clan.get("members", [])) >= CLAN_MAX: raise HTTPException(400, "Full")
    await db.clans.update_one({"_id": oid}, {"$addToSet": {"members": un}})
    await db.users.update_one({"username": un}, {"$set": {"clan_id": oid}})
    return {"ok": True}


@app.post("/api/clans/leave")
async def leave_clan(d: ClanLeave):
    un = decode_token(d.token)
    if not un: raise HTTPException(401, "Login required")
    u = await db.users.find_one({"username": un})
    if not u or not u.get("clan_id"): raise HTTPException(400, "Not in a clan")
    cid = u["clan_id"]
    clan = await db.clans.find_one({"_id": cid})
    if clan:
        await db.clans.update_one({"_id": cid}, {"$pull": {"members": un}})
        members = [m for m in clan.get("members", []) if m != un]
        if clan.get("leader") == un:
            if members:
                await db.clans.update_one({"_id": cid}, {"$set": {"leader": members[0]}})
            else:
                await db.clans.delete_one({"_id": cid})
    await db.users.update_one({"username": un}, {"$set": {"clan_id": None}})
    return {"ok": True}


# ---------- EARN ----------
class EarnStart(BaseModel):
    token: Optional[str] = None

class EarnSubmit(BaseModel):
    challenge_id: str
    answer: str
    token: Optional[str] = None


@app.post("/api/earn/start")
async def earn_start(d: EarnStart = EarnStart()):
    user = username = None
    if d.token:
        username = decode_token(d.token)
        if username: user = await db.users.find_one({"username": username})
    correct = random.choice(ANIMALS)
    options = [correct["name"]] + [a["name"] for a in random.sample([a for a in ANIMALS if a["id"] != correct["id"]], 8)]
    random.shuffle(options)
    cid = secrets.token_hex(8)
    now = time.time()
    if user:
        await db.users.update_one({"username": username}, {"$set": {"earn_challenge": {"id": cid, "animal": correct["id"], "ts": now}}})
    else:
        guest_challenges[cid] = {"animal": correct["id"], "ts": now}
    return {
        "challenge_id": cid, "animal_id": correct["id"],
        "svg": SVG.get(correct["id"], SVG["default"]),
        "emoji": correct["emoji"], "options": options,
    }


@app.post("/api/earn/submit")
async def earn_submit(d: EarnSubmit):
    user = username = None
    if d.token:
        username = decode_token(d.token)
        if username: user = await db.users.find_one({"username": username})
    now = time.time()
    correct_id = None
    if user and user.get("earn_challenge"):
        ch = user["earn_challenge"]
        if ch["id"] != d.challenge_id: raise HTTPException(400, "Invalid challenge")
        if now - ch["ts"] > 60: raise HTTPException(400, "Expired")
        correct_id = ch["animal"]
        await db.users.update_one({"username": username}, {"$unset": {"earn_challenge": ""}})
    else:
        ch = guest_challenges.get(d.challenge_id)
        if not ch: raise HTTPException(400, "Invalid challenge")
        if now - ch["ts"] > 60: raise HTTPException(400, "Expired")
        correct_id = ch["animal"]
        del guest_challenges[d.challenge_id]
    animal = next(a for a in ANIMALS if a["id"] == correct_id)
    ok = d.answer.lower() == animal["name"].lower()
    reward, cd = 0, EARN_FAIL_CD
    if ok:
        cd = EARN_OK_CD
        reward = EARN_REWARD
        if user:
            mx = await calc_max(user)
            add = min(reward, mx - user.get("pixels", 0))
            if add > 0:
                await db.users.update_one({"username": username}, {"$inc": {"pixels": add}, "$set": {"last_earn": now}})
                reward = add
            else:
                reward = 0
    return {"correct": ok, "reward": reward if ok else 0, "cooldown": cd, "correct_name": animal["name"]}


# ---------- CHAT ----------
class ChatMsg(BaseModel):
    channel: str
    text: str
    token: Optional[str] = None


@app.post("/api/chat")
async def send_chat(m: ChatMsg):
    if m.channel not in ("global", "ru", "en", "ar", "tr"):
        raise HTTPException(400, "Invalid channel")
    if not m.text.strip() or len(m.text) > 200:
        raise HTTPException(400, "Invalid message")
    username = "Guest"
    if m.token:
        un = decode_token(m.token)
        if un:
            u = await db.users.find_one({"username": un})
            if u:
                if u.get("banned"): raise HTTPException(403, "Banned")
                if u.get("muted_until", 0) > time.time(): raise HTTPException(403, "Muted")
                username = u.get("display_name", un)
    doc = {"channel": m.channel, "user": username, "text": m.text.strip(), "ts": time.time()}
    await db.chat.insert_one(doc)
    await broadcast({"type": "chat", **doc})
    return {"ok": True}


@app.get("/api/chat/{channel}")
async def get_chat(channel: str, limit: int = 50):
    msgs = []
    async for m in db.chat.find({"channel": channel}).sort("ts", -1).limit(limit):
        msgs.append({"user": m["user"], "text": m["text"], "ts": m["ts"]})
    msgs.reverse()
    return {"messages": msgs}


# ---------- EVENT ----------
async def run_color_event():
    global current_event
    current_event = {"active": True, "options": ["red", "blue", "green", "orange"],
                     "votes": {"red": 0, "blue": 0, "green": 0, "orange": 0}, "winner": None}
    await broadcast({"type": "event_start", "options": current_event["options"]})
    await asyncio.sleep(300)
    winner = max(current_event["votes"], key=current_event["votes"].get)
    current_event["winner"] = winner
    current_event["active"] = False
    await broadcast({"type": "event_end", "winner": winner, "votes": current_event["votes"]})


@app.post("/api/event/vote")
async def vote(color: str, token: str):
    if not current_event["active"]: raise HTTPException(400, "No event")
    if color not in current_event["votes"]: raise HTTPException(400, "Bad color")
    if not decode_token(token): raise HTTPException(401, "Login required")
    current_event["votes"][color] += 1
    return {"ok": True}


# ---------- WS ----------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    await broadcast({"type": "online", "count": len(ws_clients)})
    try:
        while True:
            data = await ws.receive_json()
            if data.get("type") == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(ws)
        await broadcast({"type": "online", "count": len(ws_clients)})


@app.get("/api/online")
async def online():
    return {"count": len(ws_clients)}


@app.get("/api/info")
async def info():
    return {"width": CANVAS_W, "height": CANVAS_H, "palette_size": len(PALETTE), "chunk_size": CHUNK}


# ---------- SERVE static ----------
BASE = Path(__file__).resolve().parent

@app.get("/")
async def index():
    p = BASE / "index.html"
    if p.exists():
        return FileResponse(p)
    return HTMLResponse("<h1>Put index.html next to main.py</h1>")


@app.get("/world_map.png")
async def world_map():
    p = BASE / "world_map.png"
    if p.exists():
        return FileResponse(p, media_type="image/png")
    raise HTTPException(404, "Map not found")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
