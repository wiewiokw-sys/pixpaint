
import os
import random
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Query, Body
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError, jwt
from passlib.context import CryptContext
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from pydantic import BaseModel, Field

SECRET_KEY = os.getenv("SECRET_KEY", "pixpaint-change-this-secret-key-immediately-237360")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 30
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("WARNING: MONGO_URI not set!")

CANVAS_SIZE = 4096
MAX_PIXELS_GUEST = 0
MAX_PIXELS_AUTH = 200
MAX_PIXELS_DISCORD_BONUS = 100
MAX_PIXELS_MOD_BONUS = 15
CLAN_BONUS_PER_MEMBER = 5
CLAN_MAX_MEMBERS = 20
ACCOUNT_MEDAL_BONUS = 10
STREAK_MEDAL_BONUS = 20
MOD_CODE = "237360049320122092250232257"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

client = None
db = None
users_col = None
pixels_col = None
clans_col = None
chats_col = None
stats_col = None

online_count = 0

class PlacePixel(BaseModel):
    x: int
    y: int
    color: str

class PlaceBatch(BaseModel):
    pixels: List[PlacePixel]

class EarnCheck(BaseModel):
    answer: str

class ClearWater(BaseModel):
    x1: int = 0
    y1: int = 0
    x2: int = 0
    y2: int = 0
    full: bool = False
    color: Optional[str] = None

class ReplaceColor(BaseModel):
    from_color: str
    to_color: str

class ModAction(BaseModel):
    target: str
    minutes: Optional[int] = 30
    reason: Optional[str] = ""

class ClanCreate(BaseModel):
    name: str
    tag: str  # @clan

class BecomeMod(BaseModel):
    code: str

class RegisterBody(BaseModel):
    display_name: str
    username: str
    password: str

class UpdateProfile(BaseModel):
    display_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db, users_col, pixels_col, clans_col, chats_col, stats_col
    if MONGO_URI:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["pixpaint"]
        users_col = db["users"]
        pixels_col = db["pixels"]
        clans_col = db["clans"]
        chats_col = db["chats"]
        stats_col = db["stats"]
        try:
            pixels_col.create_index([("x", ASCENDING), ("y", ASCENDING)], unique=True)
            users_col.create_index("username", unique=True)
            clans_col.create_index("tag", unique=True)
        except Exception as e:
            print("Index warning:", e)
        print("MongoDB connected")
    yield
    if client:
        client.close()

app = FastAPI(title="PixPaint", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []
    async def connect(self, ws: WebSocket):
        global online_count
        await ws.accept()
        self.active.append(ws)
        online_count = len(self.active)
    def disconnect(self, ws: WebSocket):
        global online_count
        if ws in self.active:
            self.active.remove(ws)
        online_count = len(self.active)
    async def broadcast(self, message: dict):
        dead = []
        data = json.dumps(message, default=str)
        for ws in self.active:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for d in dead:
            self.disconnect(d)

manager = ConnectionManager()

def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: Optional[str] = Depends(oauth2_scheme)):
    if not token or users_col is None:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            return None
        return users_col.find_one({"username": username})
    except JWTError:
        return None

def get_pixel_limit(user) -> int:
    if not user:
        return 0
    limit = MAX_PIXELS_AUTH + ACCOUNT_MEDAL_BONUS  # has account
    if user.get("discord_medal"):
        limit += MAX_PIXELS_DISCORD_BONUS
    if user.get("is_mod"):
        limit += MAX_PIXELS_MOD_BONUS
    if user.get("streak_medal"):
        limit += STREAK_MEDAL_BONUS
    clan_id = user.get("clan_id")
    if clan_id and clans_col is not None:
        try:
            clan = clans_col.find_one({"_id": ObjectId(clan_id)})
            if clan:
                members = len(clan.get("members", []))
                limit += min(members, CLAN_MAX_MEMBERS) * CLAN_BONUS_PER_MEMBER
        except Exception:
            pass
    return limit

def user_public(user: dict) -> dict:
    return {
        "display_name": user.get("display_name") or user.get("username"),
        "username": user.get("username"),
        "is_mod": user.get("is_mod", False),
        "discord_medal": user.get("discord_medal", False),
        "streak_medal": user.get("streak_medal", False),
        "clan_id": user.get("clan_id"),
        "clan_tag": user.get("clan_tag"),
        "pixels_left": user.get("pixels_left", 0),
        "pixels_placed": user.get("pixels_placed", 0),
        "earn_streak": user.get("earn_streak", 0),
        "banned": user.get("banned", False),
        "medals": {
            "account": True,
            "discord": bool(user.get("discord_medal")),
            "mod": bool(user.get("is_mod")),
            "clan": bool(user.get("clan_id")),
            "streak": bool(user.get("streak_medal")),
        }
    }

def _find_file(name: str) -> Optional[str]:
    base = os.path.dirname(os.path.abspath(__file__))
    for p in [os.path.join(base, name), os.path.join(os.getcwd(), name), os.path.join("/opt/render/project/src", name), name]:
        try:
            if p and os.path.isfile(p):
                return os.path.abspath(p)
        except Exception:
            pass
    return None

# ---------- AUTH ----------
@app.post("/register")
async def register(data: RegisterBody):
    if users_col is None:
        raise HTTPException(503, "DB not ready")
    uname = data.username.strip().lstrip("@").lower()
    dname = data.display_name.strip()
    if len(uname) < 3 or len(uname) > 20:
        raise HTTPException(400, "Username 3-20 chars")
    if len(dname) < 1 or len(dname) > 32:
        raise HTTPException(400, "Display name 1-32 chars")
    if len(data.password) < 4:
        raise HTTPException(400, "Password min 4 chars")
    if users_col.find_one({"username": uname}):
        raise HTTPException(400, "Username taken")
    users_col.insert_one({
        "username": uname,
        "display_name": dname,
        "password": get_password_hash(data.password),
        "pixels_left": MAX_PIXELS_AUTH + ACCOUNT_MEDAL_BONUS,
        "pixels_placed": 0,
        "is_mod": False,
        "discord_medal": False,
        "streak_medal": False,
        "earn_streak": 0,
        "banned": False,
        "muted_until": None,
        "no_place_until": None,
        "no_place_reason": None,
        "no_place_by": None,
        "clan_id": None,
        "clan_tag": None,
        "created_at": datetime.utcnow()
    })
    return {"ok": True}

@app.post("/token")
async def login(form: OAuth2PasswordRequestForm = Depends()):
    if users_col is None:
        raise HTTPException(503, "DB not ready")
    uname = form.username.strip().lstrip("@").lower()
    user = users_col.find_one({"username": uname})
    if not user or not verify_password(form.password, user["password"]):
        raise HTTPException(401, "Wrong username or password")
    if user.get("banned"):
        raise HTTPException(403, "Account banned")
    token = create_access_token({"sub": user["username"]})
    return {"access_token": token, "token_type": "bearer", "user": {**user_public(user), "limit": get_pixel_limit(user)}}

@app.get("/me")
async def me(user=Depends(get_current_user)):
    if not user:
        return {"guest": True, "pixels_left": 0, "limit": 0, "online": online_count}
    limit = get_pixel_limit(user)
    return {"guest": False, **user_public(user), "limit": limit, "online": online_count}

@app.post("/profile/update")
async def profile_update(data: UpdateProfile, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    upd = {}
    if data.display_name is not None:
        dn = data.display_name.strip()
        if not dn or len(dn) > 32:
            raise HTTPException(400, "Bad display name")
        upd["display_name"] = dn
    if data.username is not None:
        un = data.username.strip().lstrip("@").lower()
        if len(un) < 3 or len(un) > 20:
            raise HTTPException(400, "Username 3-20")
        if users_col.find_one({"username": un, "_id": {"$ne": user["_id"]}}):
            raise HTTPException(400, "Username taken")
        upd["username"] = un
    if data.password is not None:
        if len(data.password) < 4:
            raise HTTPException(400, "Password min 4")
        upd["password"] = get_password_hash(data.password)
    if not upd:
        raise HTTPException(400, "Nothing to update")
    users_col.update_one({"_id": user["_id"]}, {"$set": upd})
    return {"ok": True}

# ---------- PIXELS ----------
@app.post("/place")
async def place_pixel(data: PlacePixel, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "NOT_AUTHORIZED")
    if user.get("banned"):
        raise HTTPException(403, "Banned")
    np = user.get("no_place_until")
    if np and np > datetime.utcnow():
        raise HTTPException(403, f"Restricted by @{user.get('no_place_by','mod')}: {user.get('no_place_reason','')}")
    if user.get("pixels_left", 0) <= 0:
        raise HTTPException(400, "No pixels left")
    x, y, color = data.x, data.y, (data.color or "").strip()
    if not (0 <= x < CANVAS_SIZE and 0 <= y < CANVAS_SIZE):
        raise HTTPException(400, "Out of bounds")
    if not (color.startswith("#") and len(color) in (4, 7)):
        raise HTTPException(400, "Invalid color")
    users_col.update_one({"_id": user["_id"]}, {"$inc": {"pixels_left": -1, "pixels_placed": 1}})
    if pixels_col is not None:
        pixels_col.update_one({"x": x, "y": y}, {"$set": {
            "color": color, "user_id": str(user["_id"]),
            "username": user["username"], "display_name": user.get("display_name") or user["username"],
            "placed_at": datetime.utcnow()
        }}, upsert=True)
    msg = {"type": "pixel", "x": x, "y": y, "color": color, "username": user["username"]}
    await manager.broadcast(msg)
    return {"ok": True, "pixels_left": user.get("pixels_left", 1) - 1}

@app.post("/place_batch")
async def place_batch(data: PlaceBatch, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "NOT_AUTHORIZED")
    if user.get("banned"):
        raise HTTPException(403, "Banned")
    np = user.get("no_place_until")
    if np and np > datetime.utcnow():
        raise HTTPException(403, f"Restricted by @{user.get('no_place_by','mod')}")
    pts = data.pixels[:25]  # max 5x5
    need = len(pts)
    if user.get("pixels_left", 0) < need:
        raise HTTPException(400, "Not enough pixels")
    placed = []
    for p in pts:
        x, y, color = p.x, p.y, (p.color or "").strip()
        if not (0 <= x < CANVAS_SIZE and 0 <= y < CANVAS_SIZE):
            continue
        if not (color.startswith("#") and len(color) in (4, 7)):
            continue
        if pixels_col is not None:
            pixels_col.update_one({"x": x, "y": y}, {"$set": {
                "color": color, "user_id": str(user["_id"]),
                "username": user["username"], "display_name": user.get("display_name") or user["username"],
                "placed_at": datetime.utcnow()
            }}, upsert=True)
        placed.append({"x": x, "y": y, "color": color})
    if placed:
        users_col.update_one({"_id": user["_id"]}, {"$inc": {"pixels_left": -len(placed), "pixels_placed": len(placed)}})
        await manager.broadcast({"type": "pixels", "pixels": placed, "username": user["username"]})
    return {"ok": True, "placed": len(placed), "pixels_left": user.get("pixels_left", 0) - len(placed)}

@app.get("/lookup")
async def lookup(x: int = Query(...), y: int = Query(...)):
    if pixels_col is None:
        return {"empty": True}
    px = pixels_col.find_one({"x": x, "y": y})
    if not px:
        return {"empty": True}
    info = {
        "display_name": px.get("display_name") or px.get("username"),
        "username": px.get("username"),
        "color": px.get("color"),
        "placed_at": px.get("placed_at"),
        "pixels_left": 0, "limit": 0, "medals": {}
    }
    if px.get("user_id") and users_col is not None:
        try:
            u = users_col.find_one({"_id": ObjectId(px["user_id"])})
            if u:
                info.update(user_public(u))
                info["limit"] = get_pixel_limit(u)
        except Exception:
            pass
    return info

@app.get("/canvas/chunk")
async def get_chunk(x1: int = 0, y1: int = 0, x2: int = 512, y2: int = 512):
    if pixels_col is None:
        return []
    x1, y1 = max(0, x1), max(0, y1)
    x2, y2 = min(max(x2, x1+1), CANVAS_SIZE), min(max(y2, y1+1), CANVAS_SIZE)
    if (x2 - x1) * (y2 - y1) > 2_000_000:
        raise HTTPException(400, "Chunk too large")
    return list(pixels_col.find(
        {"x": {"$gte": x1, "$lt": x2}, "y": {"$gte": y1, "$lt": y2}},
        {"_id": 0, "x": 1, "y": 1, "color": 1}
    ).limit(500000))

@app.get("/stats")
async def stats():
    total_pixels = 0
    if pixels_col is not None:
        total_pixels = pixels_col.count_documents({})
    return {"online": online_count, "pixels": total_pixels}

# ---------- MOD ----------
@app.post("/mod/become")
async def become_mod(data: BecomeMod, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    if (data.code or "").strip() != MOD_CODE:
        raise HTTPException(403, "Wrong code")
    users_col.update_one({"_id": user["_id"]}, {
        "$set": {"is_mod": True},
        "$inc": {"pixels_left": MAX_PIXELS_MOD_BONUS}
    })
    return {"ok": True}

@app.post("/mod/ban")
async def mod_ban(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = data.target.strip().lstrip("@").lower()
    users_col.update_one({"username": target}, {"$set": {
        "banned": True,
        "ban_reason": data.reason or "",
        "banned_by": user["username"]
    }})
    return {"ok": True}

@app.post("/mod/no_place")
async def mod_no_place(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = data.target.strip().lstrip("@").lower()
    minutes = max(1, min(data.minutes or 60, 2880))
    until = datetime.utcnow() + timedelta(minutes=minutes)
    users_col.update_one({"username": target}, {"$set": {
        "no_place_until": until,
        "no_place_reason": data.reason or "",
        "no_place_by": user["username"]
    }})
    return {"ok": True, "until": until}

@app.post("/mod/mute")
async def mod_mute(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = data.target.strip().lstrip("@").lower()
    until = datetime.utcnow() + timedelta(minutes=data.minutes or 30)
    users_col.update_one({"username": target}, {"$set": {"muted_until": until}})
    return {"ok": True}

@app.post("/mod/clear_water")
async def clear_water(data: ClearWater, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    if pixels_col is None:
        raise HTTPException(503, "DB not ready")
    if data.full:
        pixels_col.delete_many({})
        await manager.broadcast({"type": "clear_all"})
        return {"ok": True, "msg": "Full clear"}
    x_min, x_max = min(data.x1, data.x2), max(data.x1, data.x2)
    y_min, y_max = min(data.y1, data.y2), max(data.y1, data.y2)
    q = {"x": {"$gte": x_min, "$lte": x_max}, "y": {"$gte": y_min, "$lte": y_max}}
    if data.color:
        q["color"] = data.color.strip()
    result = pixels_col.delete_many(q)
    await manager.broadcast({"type": "clear", "x1": x_min, "y1": y_min, "x2": x_max, "y2": y_max})
    return {"ok": True, "deleted": result.deleted_count}

# ---------- EARN ----------
ANIMALS = ["cat","dog","tiger","bear","antelope","elephant","hippo","crocodile","human","mosquito","pig","fox","wolf","rabbit","owl","snake","deer","lion","panda","koala"]
earn_sessions: Dict[str, str] = {}


@app.post("/mod/replace_color")
async def replace_color(data: ReplaceColor, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    if pixels_col is None:
        raise HTTPException(503, "DB not ready")
    fr = (data.from_color or "").strip().lower()
    to = (data.to_color or "").strip().lower()
    if not fr.startswith("#") or not to.startswith("#"):
        raise HTTPException(400, "Need #hex colors")
    # normalize short hex
    def norm(c):
        c = c.lower()
        if len(c) == 4:
            c = "#" + c[1]*2 + c[2]*2 + c[3]*2
        return c
    fr, to = norm(fr), norm(to)
    result = pixels_col.update_many({"color": fr}, {"$set": {"color": to}})
    # also try original case variants
    if result.modified_count == 0:
        result = pixels_col.update_many({"color": {"$regex": f"^{fr}$", "$options": "i"}}, {"$set": {"color": to}})
    await manager.broadcast({"type": "recolor", "from": fr, "to": to})
    return {"ok": True, "modified": result.modified_count}

@app.get("/earn/start")
async def earn_start(user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    correct = random.choice(ANIMALS)
    options = random.sample([a for a in ANIMALS if a != correct], 8) + [correct]
    random.shuffle(options)
    sid = str(random.randint(100000, 999999))
    earn_sessions[sid] = correct
    if len(earn_sessions) > 20000:
        earn_sessions.clear()
    return {"session_id": sid, "options": options, "animal": correct, "cooldown": 15}

@app.post("/earn/check")
async def earn_check(data: EarnCheck, session_id: str = Query(...), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    correct = earn_sessions.pop(session_id, None)
    if not correct:
        raise HTTPException(400, "Session expired")
    if data.answer.lower() != correct.lower():
        users_col.update_one({"_id": user["_id"]}, {"$set": {"earn_streak": 0}})
        return {"ok": False, "next_cooldown": 15, "correct": correct}
    streak = int(user.get("earn_streak") or 0) + 1
    # +15 + 0.05 per streak step, max 20; store as int cents * 100 for pixels as float display
    bonus = min(20.0, 15.0 + (streak - 1) * 0.05)
    add = int(round(bonus))  # pixels are ints; fractional for display
    # actually user wants fractional accumulation conceptually but pixels are whole - use floor of bonus
    add = max(15, min(20, int(15 + (streak - 1) * 0.05)))
    upd = {"$inc": {"pixels_left": add}, "$set": {"earn_streak": streak}}
    if streak >= 5:
        upd["$set"]["streak_medal"] = True
    users_col.update_one({"_id": user["_id"]}, upd)
    return {"ok": True, "added": add, "bonus": bonus, "streak": streak, "next_cooldown": 15}

# ---------- CLANS ----------
@app.post("/clan/create")
async def create_clan(data: ClanCreate, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    if user.get("clan_id"):
        raise HTTPException(400, "Already in a clan")
    tag = data.tag.strip().lstrip("@").lower()
    name = data.name.strip()[:32]
    if len(tag) < 2 or len(tag) > 16:
        raise HTTPException(400, "Tag 2-16 chars")
    try:
        res = clans_col.insert_one({
            "name": name, "tag": tag, "leader": user["username"],
            "members": [user["username"]], "created_at": datetime.utcnow()
        })
    except DuplicateKeyError:
        raise HTTPException(400, "Tag taken")
    users_col.update_one({"_id": user["_id"]}, {"$set": {"clan_id": str(res.inserted_id), "clan_tag": tag}})
    return {"ok": True, "clan_id": str(res.inserted_id), "tag": tag}

@app.get("/clan/search")
async def clan_search(tag: str = Query(...)):
    tag = tag.strip().lstrip("@").lower()
    c = clans_col.find_one({"tag": tag}) if clans_col is not None else None
    if not c:
        raise HTTPException(404, "Clan not found")
    return {"id": str(c["_id"]), "name": c["name"], "tag": c["tag"], "leader": c["leader"], "members_count": len(c.get("members", []))}

@app.post("/clan/join")
async def join_clan(tag: str = Body(..., embed=True), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    if user.get("clan_id"):
        raise HTTPException(400, "Already in a clan")
    tag = tag.strip().lstrip("@").lower()
    clan = clans_col.find_one({"tag": tag})
    if not clan:
        raise HTTPException(404, "Clan not found")
    if len(clan.get("members", [])) >= CLAN_MAX_MEMBERS:
        raise HTTPException(400, "Clan full")
    clans_col.update_one({"_id": clan["_id"]}, {"$addToSet": {"members": user["username"]}})
    users_col.update_one({"_id": user["_id"]}, {"$set": {"clan_id": str(clan["_id"]), "clan_tag": tag}})
    return {"ok": True}

@app.get("/clans")
async def list_clans():
    if clans_col is None:
        return []
    out = []
    for c in clans_col.find({}, {"name": 1, "tag": 1, "leader": 1, "members": 1}).limit(50):
        out.append({"id": str(c["_id"]), "name": c["name"], "tag": c.get("tag", ""), "leader": c["leader"], "members_count": len(c.get("members", []))})
    return out

# ---------- CHAT ----------
@app.post("/chat")
async def send_chat(message: str = Body(..., embed=True), channel: str = Body("global"), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    muted = user.get("muted_until")
    if muted and muted > datetime.utcnow():
        raise HTTPException(403, "Muted")
    if len(message) > 200:
        raise HTTPException(400, "Too long")
    if channel not in ("global", "en", "ru", "ar", "tr"):
        channel = "global"
    doc = {
        "username": user["username"],
        "display_name": user.get("display_name") or user["username"],
        "message": message[:200],
        "channel": channel,
        "is_mod": user.get("is_mod", False),
        "created_at": datetime.utcnow()
    }
    if chats_col is not None:
        chats_col.insert_one(doc)
    await manager.broadcast({"type": "chat", **{k: v for k, v in doc.items() if k != "_id"}, "created_at": str(doc["created_at"])})
    return {"ok": True}

@app.get("/chat/history")
async def chat_history(channel: str = "global", limit: int = 50):
    if chats_col is None:
        return []
    cursor = chats_col.find({"channel": channel}).sort("created_at", DESCENDING).limit(min(limit, 100))
    return [{"username": c["username"], "display_name": c.get("display_name"), "message": c["message"], "is_mod": c.get("is_mod", False), "created_at": str(c["created_at"])} for c in cursor][::-1]

# ---------- WS / STATIC ----------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)

@app.get("/")
async def root():
    p = _find_file("index.html")
    if p:
        return FileResponse(p, media_type="text/html")
    return HTMLResponse("<h2>index.html not found</h2><p>Push index.html to GitHub and redeploy.</p>", status_code=404)

@app.get("/world_map.png")
async def world_map():
    p = _find_file("world_map.png")
    if p:
        return FileResponse(p, media_type="image/png")
    raise HTTPException(404, "world_map.png not found")

@app.get("/health")
async def health():
    base = os.path.dirname(os.path.abspath(__file__))
    try:
        listing = os.listdir(base)
    except Exception as e:
        listing = [str(e)]
    return {"status": "ok", "mongo": users_col is not None, "index_html": _find_file("index.html") is not None, "world_map": _find_file("world_map.png") is not None, "files": listing, "online": online_count}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
