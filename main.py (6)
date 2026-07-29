import os
import random
import asyncio
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
from pydantic import BaseModel

# ================== CONFIG ==================
SECRET_KEY = os.getenv("SECRET_KEY", "pixpaint-change-this-secret-key-immediately-237360")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 30

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    # Для локального тесту можна поставити dummy, але на Render обов'язково
    print("WARNING: MONGO_URI not set!")

CANVAS_SIZE = 4096
MAX_PIXELS_GUEST = 60
MAX_PIXELS_AUTH = 200
MAX_PIXELS_DISCORD_BONUS = 100
MAX_PIXELS_MOD_BONUS = 15
CLAN_BONUS_PER_MEMBER = 5
CLAN_MAX_MEMBERS = 20

MOD_CODE = "237360049320122092250232257"

# 50 кольорів (спектр від червоного)
PALETTE = [
    "#FF0000", "#FF1A00", "#FF3300", "#FF4D00", "#FF6600",
    "#FF8000", "#FF9900", "#FFB300", "#FFCC00", "#FFE600",
    "#FFFF00", "#E6FF00", "#CCFF00", "#B3FF00", "#99FF00",
    "#80FF00", "#66FF00", "#4DFF00", "#33FF00", "#1AFF00",
    "#00FF00", "#00FF1A", "#00FF33", "#00FF4D", "#00FF66",
    "#00FF80", "#00FF99", "#00FFB3", "#00FFCC", "#00FFE6",
    "#00FFFF", "#00E6FF", "#00CCFF", "#00B3FF", "#0099FF",
    "#0080FF", "#0066FF", "#004DFF", "#0033FF", "#001AFF",
    "#0000FF", "#1A00FF", "#3300FF", "#4D00FF", "#6600FF",
    "#8000FF", "#9900FF", "#B300FF", "#CC00FF", "#E600FF",
    "#FF00FF", "#FFFFFF", "#C0C0C0", "#808080", "#000000"
]

ANIMALS = [
    "cat", "dog", "tiger", "bear", "antelope", "elephant",
    "hippo", "crocodile", "human", "mosquito", "pig",
    "fox", "wolf", "rabbit", "owl", "snake", "deer",
    "lion", "panda", "koala"
]

# ================== MODELS ==================
class PlacePixel(BaseModel):
    x: int
    y: int
    color: str

class EarnCheck(BaseModel):
    answer: str
    correct: str

class ClearWater(BaseModel):
    x1: int
    y1: int
    x2: int
    y2: int
    full: bool = False

class ModAction(BaseModel):
    target: str
    minutes: Optional[int] = 30

class ClanCreate(BaseModel):
    name: str

class BecomeMod(BaseModel):
    code: str

# ================== APP ==================
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

client = None
db = None
users_col = None
pixels_col = None
clans_col = None
chats_col = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db, users_col, pixels_col, clans_col, chats_col
    if MONGO_URI:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["pixpaint"]
        users_col = db["users"]
        pixels_col = db["pixels"]
        clans_col = db["clans"]
        chats_col = db["chats"]
        # Індекси
        try:
            pixels_col.create_index([("x", ASCENDING), ("y", ASCENDING)], unique=True)
            users_col.create_index("username", unique=True)
            clans_col.create_index("name", unique=True)
        except Exception as e:
            print(f"Index warning: {e}")
        print("MongoDB connected")
    else:
        print("Running without MongoDB (local test mode)")
    yield
    if client:
        client.close()

app = FastAPI(title="PixPaint", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================== WS MANAGER ==================
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

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

# ================== HELPERS ==================
def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: Optional[str] = Depends(oauth2_scheme)) -> Optional[dict]:
    if not token or users_col is None:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if not username:
            return None
        user = users_col.find_one({"username": username})
        return user
    except JWTError:
        return None

def get_pixel_limit(user: Optional[dict]) -> int:
    if not user:
        return MAX_PIXELS_GUEST
    limit = MAX_PIXELS_AUTH
    if user.get("discord_medal"):
        limit += MAX_PIXELS_DISCORD_BONUS
    if user.get("is_mod"):
        limit += MAX_PIXELS_MOD_BONUS
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
        "username": user.get("username"),
        "is_mod": user.get("is_mod", False),
        "discord_medal": user.get("discord_medal", False),
        "medals": user.get("medals", []),
        "pixels_left": user.get("pixels_left", 0),
        "clan_id": user.get("clan_id"),
        "banned": user.get("banned", False),
    }

# ================== AUTH ==================
@app.post("/register")
async def register(form: OAuth2PasswordRequestForm = Depends()):
    if users_col is None:
        raise HTTPException(503, "DB not ready")
    if users_col.find_one({"username": form.username}):
        raise HTTPException(400, "Username already taken")
    if len(form.username) < 3 or len(form.username) > 20:
        raise HTTPException(400, "Username 3-20 chars")
    hashed = get_password_hash(form.password)
    users_col.insert_one({
        "username": form.username,
        "password": hashed,
        "pixels_left": MAX_PIXELS_AUTH,
        "is_mod": False,
        "discord_medal": False,
        "banned": False,
        "muted_until": None,
        "no_place_until": None,
        "clan_id": None,
        "medals": [],
        "lang": "en",
        "created_at": datetime.utcnow()
    })
    return {"ok": True, "msg": "Registered"}

@app.post("/token")
async def login(form: OAuth2PasswordRequestForm = Depends()):
    if users_col is None:
        raise HTTPException(503, "DB not ready")
    user = users_col.find_one({"username": form.username})
    if not user or not verify_password(form.password, user["password"]):
        raise HTTPException(401, "Wrong username or password")
    if user.get("banned"):
        raise HTTPException(403, "Account banned")
    token = create_access_token({"sub": user["username"]})
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": user_public(user)
    }

@app.get("/me")
async def me(user=Depends(get_current_user)):
    if not user:
        return {"guest": True, "pixels_left": MAX_PIXELS_GUEST, "limit": MAX_PIXELS_GUEST}
    limit = get_pixel_limit(user)
    return {
        "guest": False,
        **user_public(user),
        "limit": limit,
        "pixels_left": user.get("pixels_left", 0)
    }

# ================== PIXELS ==================
@app.post("/place")
async def place_pixel(data: PlacePixel, user=Depends(get_current_user)):
    x, y, color = data.x, data.y, data.color
    if not (0 <= x < CANVAS_SIZE and 0 <= y < CANVAS_SIZE):
        raise HTTPException(400, "Coords out of bounds")
    # приймаємо #RGB / #RRGGBB (палітра на клієнті)
    c = (color or "").strip()
    if not (c.startswith("#") and len(c) in (4, 7)):
        raise HTTPException(400, "Invalid color")
    color = c

    username = "guest"
    uid = None

    if user:
        if user.get("banned"):
            raise HTTPException(403, "Banned")
        no_place = user.get("no_place_until")
        if no_place and no_place > datetime.utcnow():
            raise HTTPException(403, "You are restricted from placing pixels")
        if user.get("pixels_left", 0) <= 0:
            raise HTTPException(400, "No pixels left. Earn more!")
        users_col.update_one({"_id": user["_id"]}, {"$inc": {"pixels_left": -1}})
        username = user["username"]
        uid = str(user["_id"])
    else:
        # Гості теж можуть, але ліміт клієнтський + серверний soft
        pass

    if pixels_col is not None:
        pixels_col.update_one(
            {"x": x, "y": y},
            {"$set": {
                "color": color,
                "user_id": uid,
                "username": username,
                "placed_at": datetime.utcnow()
            }},
            upsert=True
        )

    msg = {
        "type": "pixel",
        "x": x, "y": y,
        "color": color,
        "username": username
    }
    await manager.broadcast(msg)
    return {"ok": True, "pixels_left": (user.get("pixels_left", 1) - 1) if user else None}

@app.get("/lookup")
async def lookup(x: int = Query(...), y: int = Query(...)):
    if pixels_col is None:
        return {"empty": True}
    px = pixels_col.find_one({"x": x, "y": y})
    if not px:
        return {"empty": True}
    info = {
        "username": px.get("username", "unknown"),
        "color": px.get("color"),
        "placed_at": px.get("placed_at"),
        "is_mod": False,
        "medals": [],
        "discord_medal": False
    }
    if px.get("user_id") and users_col is not None:
        try:
            u = users_col.find_one({"_id": ObjectId(px["user_id"])})
            if u:
                info["is_mod"] = u.get("is_mod", False)
                info["medals"] = u.get("medals", [])
                info["discord_medal"] = u.get("discord_medal", False)
        except Exception:
            pass
    return info

@app.get("/canvas/chunk")
async def get_chunk(x1: int = 0, y1: int = 0, x2: int = 512, y2: int = 512):
    """Повертає шматок канвасу для початкового завантаження"""
    if pixels_col is None:
        return []
    x1 = max(0, x1); y1 = max(0, y1)
    x2 = min(max(x2, x1+1), CANVAS_SIZE)
    y2 = min(max(y2, y1+1), CANVAS_SIZE)
    # обмеження щоб не вбити RAM
    if (x2 - x1) * (y2 - y1) > 2_000_000:
        raise HTTPException(400, "Chunk too large")
    cursor = pixels_col.find({
        "x": {"$gte": x1, "$lt": x2},
        "y": {"$gte": y1, "$lt": y2}
    }, {"_id": 0, "x": 1, "y": 1, "color": 1}).limit(500000)
    return list(cursor)

# ================== MOD ==================
@app.post("/mod/become")
async def become_mod(data: BecomeMod, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    code = (data.code or "").strip()
    if code != MOD_CODE:
        raise HTTPException(403, "Wrong code")
    users_col.update_one(
        {"_id": user["_id"]},
        {
            "$set": {"is_mod": True},
            "$addToSet": {"medals": "mod"},
            "$inc": {"pixels_left": MAX_PIXELS_MOD_BONUS}
        }
    )
    return {"ok": True, "msg": "You are now a moderator"}

@app.post("/mod/ban")
async def mod_ban(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    users_col.update_one({"username": data.target}, {"$set": {"banned": True}})
    return {"ok": True}

@app.post("/mod/delete_account")
async def mod_delete(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    users_col.delete_one({"username": data.target})
    return {"ok": True}

@app.post("/mod/mute")
async def mod_mute(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    until = datetime.utcnow() + timedelta(minutes=data.minutes or 30)
    users_col.update_one({"username": data.target}, {"$set": {"muted_until": until}})
    return {"ok": True}

@app.post("/mod/no_place")
async def mod_no_place(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    minutes = max(1, min(data.minutes or 60, 2880))  # 1 хв — 2 дні
    until = datetime.utcnow() + timedelta(minutes=minutes)
    users_col.update_one({"username": data.target}, {"$set": {"no_place_until": until}})
    return {"ok": True, "until": until}

@app.post("/mod/clear_water")
async def clear_water(data: ClearWater, user=Depends(get_current_user)):
    """Модератор очищає воду (пікселі) повністю або по координатах"""
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    if pixels_col is None:
        raise HTTPException(503, "DB not ready")

    if data.full:
        # Повне очищення всього канвасу (обережно!)
        pixels_col.delete_many({})
        await manager.broadcast({"type": "clear_all"})
        return {"ok": True, "msg": "Full canvas cleared"}
    else:
        x_min, x_max = min(data.x1, data.x2), max(data.x1, data.x2)
        y_min, y_max = min(data.y1, data.y2), max(data.y1, data.y2)
        # Обмеження розміру очистки
        if (x_max - x_min) * (y_max - y_min) > 500_000:
            raise HTTPException(400, "Area too large (max ~500k pixels)")
        result = pixels_col.delete_many({
            "x": {"$gte": x_min, "$lte": x_max},
            "y": {"$gte": y_min, "$lte": y_max}
        })
        await manager.broadcast({
            "type": "clear",
            "x1": x_min, "y1": y_min,
            "x2": x_max, "y2": y_max
        })
        return {"ok": True, "deleted": result.deleted_count}

# ================== EARN (тварини) ==================
# Тимчасове сховище correct answers (на проді краще Redis)
earn_sessions: Dict[str, str] = {}

@app.get("/earn/start")
async def earn_start(user=Depends(get_current_user)):
    correct = random.choice(ANIMALS)
    options = random.sample([a for a in ANIMALS if a != correct], 8)
    options.append(correct)
    random.shuffle(options)
    session_id = str(random.randint(100000, 999999))
    earn_sessions[session_id] = correct
    # Чистимо старі
    if len(earn_sessions) > 10000:
        earn_sessions.clear()
    return {
        "session_id": session_id,
        "options": options,
        "cooldown": 7
    }

@app.post("/earn/check")
async def earn_check(data: EarnCheck, session_id: str = Query(...), user=Depends(get_current_user)):
    correct = earn_sessions.pop(session_id, None)
    if not correct:
        raise HTTPException(400, "Session expired")
    if data.answer.lower() == correct.lower():
        add = 15
        # Тут можна додати перевірку голосування кольорів
        if user and users_col is not None:
            users_col.update_one({"_id": user["_id"]}, {"$inc": {"pixels_left": add}})
            new_left = (user.get("pixels_left") or 0) + add
        else:
            new_left = None
        return {"ok": True, "added": add, "next_cooldown": 7, "pixels_left": new_left}
    return {"ok": False, "next_cooldown": 5, "correct": correct}

# ================== CLANS ==================
@app.post("/clan/create")
async def create_clan(data: ClanCreate, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    if user.get("clan_id"):
        raise HTTPException(400, "Already in a clan")
    if clans_col is None:
        raise HTTPException(503)
    try:
        res = clans_col.insert_one({
            "name": data.name[:32],
            "leader": user["username"],
            "members": [user["username"]],
            "created_at": datetime.utcnow()
        })
        users_col.update_one({"_id": user["_id"]}, {"$set": {"clan_id": str(res.inserted_id)}})
        return {"ok": True, "clan_id": str(res.inserted_id)}
    except DuplicateKeyError:
        raise HTTPException(400, "Clan name taken")

@app.post("/clan/join")
async def join_clan(clan_id: str = Body(..., embed=True), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    if user.get("clan_id"):
        raise HTTPException(400, "Already in a clan")
    try:
        clan = clans_col.find_one({"_id": ObjectId(clan_id)})
    except Exception:
        raise HTTPException(400, "Invalid clan id")
    if not clan:
        raise HTTPException(404, "Clan not found")
    if len(clan.get("members", [])) >= CLAN_MAX_MEMBERS:
        raise HTTPException(400, "Clan full (max 20)")
    clans_col.update_one({"_id": ObjectId(clan_id)}, {"$addToSet": {"members": user["username"]}})
    users_col.update_one({"_id": user["_id"]}, {"$set": {"clan_id": clan_id}})
    return {"ok": True}

@app.get("/clans")
async def list_clans():
    if clans_col is None:
        return []
    result = []
    for c in clans_col.find({}, {"name": 1, "leader": 1, "members": 1}):
        result.append({
            "id": str(c["_id"]),
            "name": c["name"],
            "leader": c["leader"],
            "members_count": len(c.get("members", [])),
            "members": c.get("members", [])[:20]
        })
    return result

@app.post("/clan/leave")
async def leave_clan(user=Depends(get_current_user)):
    if not user or not user.get("clan_id"):
        raise HTTPException(400, "Not in a clan")
    clan_id = user["clan_id"]
    clans_col.update_one({"_id": ObjectId(clan_id)}, {"$pull": {"members": user["username"]}})
    users_col.update_one({"_id": user["_id"]}, {"$unset": {"clan_id": ""}})
    # Якщо лідер пішов і клан порожній — видаляємо
    clan = clans_col.find_one({"_id": ObjectId(clan_id)})
    if clan and len(clan.get("members", [])) == 0:
        clans_col.delete_one({"_id": ObjectId(clan_id)})
    return {"ok": True}

# ================== CHAT (базовий) ==================
@app.post("/chat")
async def send_chat(message: str = Body(..., embed=True), channel: str = Body("global"), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    muted = user.get("muted_until")
    if muted and muted > datetime.utcnow():
        raise HTTPException(403, "You are muted")
    if len(message) > 200:
        raise HTTPException(400, "Too long")
    doc = {
        "username": user["username"],
        "message": message[:200],
        "channel": channel,  # global, ru, en, ar, tr
        "is_mod": user.get("is_mod", False),
        "created_at": datetime.utcnow()
    }
    if chats_col is not None:
        chats_col.insert_one(doc)
    await manager.broadcast({"type": "chat", **doc, "created_at": str(doc["created_at"])})
    return {"ok": True}

@app.get("/chat/history")
async def chat_history(channel: str = "global", limit: int = 50):
    if chats_col is None:
        return []
    cursor = chats_col.find({"channel": channel}).sort("created_at", DESCENDING).limit(min(limit, 100))
    return [
        {
            "username": c["username"],
            "message": c["message"],
            "is_mod": c.get("is_mod", False),
            "created_at": str(c["created_at"])
        }
        for c in cursor
    ][::-1]

# ================== WEBSOCKET ==================
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            data = await ws.receive_text()
            # Можна обробляти ping
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)

# ================== STATIC / INDEX ==================
def _find_file(name: str) -> Optional[str]:
    base = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(base, name),
        os.path.join(os.getcwd(), name),
        os.path.join("/opt/render/project/src", name),
        name,
    ]
    for p in candidates:
        try:
            if p and os.path.isfile(p):
                return os.path.abspath(p)
        except Exception:
            pass
    return None


@app.get("/")
async def root():
    html_path = _find_file("index.html")
    if html_path:
        return FileResponse(html_path, media_type="text/html")
    # debug: show what files we can see
    base = os.path.dirname(os.path.abspath(__file__))
    try:
        listing = os.listdir(base)
    except Exception as e:
        listing = [str(e)]
    return HTMLResponse(
        f"<h2>index.html not found</h2>"
        f"<p>__file__ dir: {base}</p>"
        f"<p>cwd: {os.getcwd()}</p>"
        f"<p>files here: {listing}</p>"
        f"<p>Put index.html next to main.py and redeploy.</p>",
        status_code=404,
    )


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
    return {
        "status": "ok",
        "canvas": CANVAS_SIZE,
        "mongo": users_col is not None,
        "index_html": _find_file("index.html") is not None,
        "world_map": _find_file("world_map.png") is not None,
        "dir": base,
        "cwd": os.getcwd(),
        "files": listing,
    }

# Для локального запуску
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
