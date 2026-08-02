
import os
import re
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

CANVAS_SIZE = 4096
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

client = db = users_col = pixels_col = clans_col = chats_col = zones_col = public_tpl_col = news_col = None
online_count = 0
online_users: Dict[int, dict] = {}  # id(ws) -> {username, display_name, joined}
earn_sessions: Dict[str, Any] = {}
last_remote_pixel: Optional[dict] = None  # {x,y,color,username,t}
news_col = None
_pixel_cache: Optional[Dict[str, str]] = None  # "x,y" -> color
_pixel_cache_ready = False

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
    tag: str

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
    avatar_color: Optional[int] = None

class ProtectZone(BaseModel):
    x1: int
    y1: int
    x2: int
    y2: int
    minutes: int = 30
    forever: bool = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db, users_col, pixels_col, clans_col, chats_col, zones_col, public_tpl_col
    if MONGO_URI:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["pixpaint"]
        users_col = db["users"]
        pixels_col = db["pixels"]
        clans_col = db["clans"]
        chats_col = db["chats"]
        zones_col = db["zones"]
        public_tpl_col = db["public_templates"]
        global news_col
        news_col = db["news"]
        try:
            pixels_col.create_index([("x", ASCENDING), ("y", ASCENDING)], unique=True)
            users_col.create_index("username", unique=True)
            clans_col.create_index("tag", unique=True)
            chats_col.create_index("created_at")
            zones_col.create_index("expires_at")
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
        online_users[id(ws)] = {"username": "guest", "display_name": "Guest", "joined": datetime.utcnow().isoformat()}
    def disconnect(self, ws: WebSocket):
        global online_count
        if ws in self.active:
            self.active.remove(ws)
        online_users.pop(id(ws), None)
        online_count = len(self.active)
    def identify(self, ws: WebSocket, username: str, display_name: str):
        online_users[id(ws)] = {"username": username, "display_name": display_name, "joined": datetime.utcnow().isoformat()}
        if users_col is not None:
            try:
                users_col.update_one({"username": username}, {"$set": {"last_seen": datetime.utcnow()}})
            except Exception:
                pass
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

def day_key():
    return datetime.utcnow().strftime("%Y-%m-%d")

def get_ranks(user):
    """Return daily and alltime rank (1-based) by pixels_placed_day / pixels_placed"""
    if not user or users_col is None:
        return {"daily": None, "alltime": None}
    uid = user["_id"]
    # daily
    day_placed = user.get("pixels_placed_day", 0)
    day = user.get("day_key")
    if day != day_key():
        day_placed = 0
    daily_rank = users_col.count_documents({
        "$or": [
            {"day_key": day_key(), "pixels_placed_day": {"$gt": day_placed}},
            {"day_key": day_key(), "pixels_placed_day": day_placed, "_id": {"$lt": uid}}
        ]
    }) + 1 if day_placed > 0 else None
    all_placed = user.get("pixels_placed", 0)
    alltime_rank = users_col.count_documents({
        "$or": [
            {"pixels_placed": {"$gt": all_placed}},
            {"pixels_placed": all_placed, "_id": {"$lt": uid}}
        ]
    }) + 1 if all_placed > 0 else None
    return {"daily": daily_rank, "alltime": alltime_rank}

def rank_bonuses(user):
    """Compute earn bonus and limit bonus and afk rate from ranks"""
    ranks = get_ranks(user)
    earn_bonus = 0
    limit_bonus = 0
    afk_rate = 0.5  # +1 every 2 sec = 0.5/s
    d, a = ranks.get("daily"), ranks.get("alltime")
    # daily
    if d == 1:
        earn_bonus += 2
        limit_bonus += 30
    elif d == 2:
        earn_bonus += 2
    elif d == 3:
        earn_bonus += 1
    # alltime
    if a == 1:
        earn_bonus += 3
        limit_bonus += 65
        afk_rate = 2.0  # +2 per second
    elif a == 2:
        earn_bonus += 4
        limit_bonus += 60
    elif a == 3:
        earn_bonus += 3
        limit_bonus += 50
    return {"earn_bonus": earn_bonus, "limit_bonus": limit_bonus, "afk_rate": afk_rate, "ranks": ranks}

def get_pixel_limit(user) -> int:
    if not user:
        return 0
    if user.get("god_mode"):
        return 999999
    limit = MAX_PIXELS_AUTH + ACCOUNT_MEDAL_BONUS
    if user.get("discord_medal"):
        limit += MAX_PIXELS_DISCORD_BONUS
    if user.get("is_mod"):
        limit += MAX_PIXELS_MOD_BONUS
    if user.get("streak_medal"):
        limit += STREAK_MEDAL_BONUS
    if user.get("youtube_medal"):
        limit += 15000
    if user.get("pixels_2m_medal") or user.get("pixels_placed", 0) >= 2_000_000:
        limit += 10
    clan_id = user.get("clan_id")
    if clan_id and clans_col is not None:
        try:
            clan = clans_col.find_one({"_id": ObjectId(clan_id)})
            if clan:
                members = len(clan.get("members", []))
                limit += min(members, CLAN_MAX_MEMBERS) * CLAN_BONUS_PER_MEMBER
        except Exception:
            pass
    limit += rank_bonuses(user)["limit_bonus"]
    return limit

def user_public(user: dict) -> dict:
    rb = rank_bonuses(user)
    return {
        "display_name": user.get("display_name") or user.get("username"),
        "username": user.get("username"),
        "is_mod": user.get("is_mod", False),
        "god_mode": user.get("god_mode", False),
        "discord_medal": user.get("discord_medal", False),
        "streak_medal": user.get("streak_medal", False),
        "clan_id": user.get("clan_id"),
        "clan_tag": user.get("clan_tag"),
        "pixels_left": user.get("pixels_left", 0),
        "pixels_placed": user.get("pixels_placed", 0),
        "pixels_placed_day": user.get("pixels_placed_day", 0) if user.get("day_key") == day_key() else 0,
        "earn_streak": user.get("earn_streak", 0),
        "avatar_color": user.get("avatar_color", 0),
        "banned": user.get("banned", False),
        "ranks": rb["ranks"],
        "earn_bonus": rb["earn_bonus"],
        "afk_rate": rb["afk_rate"],
        "medals": {
            "account": True,
            "discord": bool(user.get("discord_medal")),
            "mod": bool(user.get("is_mod")),
            "clan": bool(user.get("clan_id")),
            "streak": bool(user.get("streak_medal")),
            "pixels_2m": bool(user.get("pixels_2m_medal") or user.get("pixels_placed", 0) >= 2_000_000),
            "youtube": bool(user.get("youtube_medal")),
        },
        "last_seen": str(user.get("last_seen") or ""),
        "last_pixel": user.get("last_pixel"),
    }

def _find_file(name: str) -> Optional[str]:
    """Search common Render / local paths for static files."""
    base = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    candidates = [
        os.path.join(base, name),
        os.path.join(cwd, name),
        os.path.join(cwd, "artifacts", name),
        os.path.join(base, "artifacts", name),
        os.path.join("/opt/render/project/src", name),
        os.path.join("/opt/render/project/src/artifacts", name),
        os.path.join("/opt/render/project", name),
        name,
        os.path.abspath(name),
    ]
    # also scan parent dirs one level
    for root in (base, cwd, "/opt/render/project/src"):
        try:
            parent = os.path.dirname(root)
            candidates.append(os.path.join(parent, name))
        except Exception:
            pass
    seen = set()
    for p in candidates:
        try:
            ap = os.path.abspath(p)
            if ap in seen:
                continue
            seen.add(ap)
            if os.path.isfile(ap):
                return ap
        except Exception:
            pass
    return None

def norm_color(c: str) -> str:
    c = (c or "").strip().lower()
    if len(c) == 4 and c.startswith("#"):
        c = "#" + c[1]*2 + c[2]*2 + c[3]*2
    return c

def check_zone(x, y):
    if zones_col is None:
        return None
    now = datetime.utcnow()
    zones_col.delete_many({"expires_at": {"$lt": now}})
    z = zones_col.find_one({
        "x1": {"$lte": x}, "x2": {"$gte": x},
        "y1": {"$lte": y}, "y2": {"$gte": y},
        "expires_at": {"$gt": now}
    })
    return z

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
        "pixels_placed_day": 0,
        "day_key": day_key(),
        "is_mod": False,
        "god_mode": False,
        "discord_medal": False,
        "streak_medal": False,
        "earn_streak": 0,
        "avatar_color": random.randint(0, 9),
        "banned": False,
        "muted_until": None,
        "no_place_until": None,
        "no_place_reason": None,
        "no_place_by": None,
        "clan_id": None,
        "clan_tag": None,
        "last_regen": datetime.utcnow(),
        "pixel_frac": 0.0,
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
    # reset daily if needed
    if user.get("day_key") != day_key():
        users_col.update_one({"_id": user["_id"]}, {"$set": {"day_key": day_key(), "pixels_placed_day": 0}})
        user["pixels_placed_day"] = 0
        user["day_key"] = day_key()
    limit = get_pixel_limit(user)
    return {"guest": False, **user_public(user), "limit": limit, "online": online_count}

class CustomColors(BaseModel):
    colors: list

@app.get("/colors")
async def get_colors(user=Depends(get_current_user)):
    if not user:
        return []
    return user.get("custom_colors") or []

@app.post("/colors")
async def set_colors(data: CustomColors, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    if user.get("pixels_placed", 0) < 2_000_000 and not user.get("is_mod"):
        raise HTTPException(403, "Need 2M pixels medal")
    cols = []
    for c in (data.colors or [])[:10]:
        c = str(c).lower().strip()
        if re.match(r"^#[0-9a-f]{6}$", c):
            cols.append(c)
    users_col.update_one({"_id": user["_id"]}, {"$set": {"custom_colors": cols}})
    return {"ok": True, "colors": cols}

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
    if data.avatar_color is not None:
        upd["avatar_color"] = max(0, min(9, int(data.avatar_color)))
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
    x, y = data.x, data.y
    color = norm_color(data.color)
    if not (0 <= x < CANVAS_SIZE and 0 <= y < CANVAS_SIZE):
        raise HTTPException(400, "Out of bounds")
    if not (color.startswith("#") and len(color) == 7):
        raise HTTPException(400, "Invalid color")
    zone = check_zone(x, y)
    if zone:
        raise HTTPException(403, f"Protected by @{zone.get('mod','mod')} until {zone.get('expires_at')}")
    # same color = free
    if pixels_col is not None:
        existing = pixels_col.find_one({"x": x, "y": y})
        if existing and norm_color(existing.get("color", "")) == color:
            return {"ok": True, "pixels_left": user.get("pixels_left", 0), "skipped": True}
    if not user.get("god_mode") and user.get("pixels_left", 0) <= 0:
        raise HTTPException(400, "No pixels left")
    if not user.get("god_mode"):
        users_col.update_one({"_id": user["_id"]}, {"$inc": {"pixels_left": -1, "pixels_placed": 1, "pixels_placed_day": 1},
            "$set": {"day_key": day_key()}})
    else:
        users_col.update_one({"_id": user["_id"]}, {"$inc": {"pixels_placed": 1, "pixels_placed_day": 1},
            "$set": {"day_key": day_key()}})
    if pixels_col is not None:
        cache_set_pixel(x, y, color)
        pixels_col.update_one({"x": x, "y": y}, {"$set": {
            "color": color, "user_id": str(user["_id"]),
            "username": user["username"], "display_name": user.get("display_name") or user["username"],
            "placed_at": datetime.utcnow()
        }}, upsert=True)
    now = datetime.utcnow()
    users_col.update_one({"_id": user["_id"]}, {"$set": {
        "last_seen": now,
        "last_pixel": {"x": x, "y": y, "color": color, "t": now.isoformat()}
    }})
    global last_remote_pixel
    last_remote_pixel = {"x": x, "y": y, "color": color, "username": user["username"], "t": now.isoformat()}
    await manager.broadcast({"type": "pixel", "x": x, "y": y, "color": color, "username": user["username"], "t": now.isoformat()})
    left = user.get("pixels_left", 1) - (0 if user.get("god_mode") else 1)
    return {"ok": True, "pixels_left": left}

@app.post("/place_batch")
async def place_batch(data: PlaceBatch, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "NOT_AUTHORIZED")
    if user.get("banned"):
        raise HTTPException(403, "Banned")
    np = user.get("no_place_until")
    if np and np > datetime.utcnow():
        raise HTTPException(403, f"Restricted by @{user.get('no_place_by','mod')}")
    pts = data.pixels[:100]
    placed = []
    skipped = 0
    for p in pts:
        x, y, color = p.x, p.y, norm_color(p.color)
        if not (0 <= x < CANVAS_SIZE and 0 <= y < CANVAS_SIZE):
            continue
        if not (color.startswith("#") and len(color) == 7):
            continue
        zone = check_zone(x, y)
        if zone:
            continue
        if pixels_col is not None:
            existing = pixels_col.find_one({"x": x, "y": y})
            if existing and norm_color(existing.get("color", "")) == color:
                skipped += 1
                continue
        if not user.get("god_mode") and user.get("pixels_left", 0) <= len(placed):
            break
        if pixels_col is not None:
            pixels_col.update_one({"x": x, "y": y}, {"$set": {
                "color": color, "user_id": str(user["_id"]),
                "username": user["username"], "display_name": user.get("display_name") or user["username"],
                "placed_at": datetime.utcnow()
            }}, upsert=True)
        cache_set_pixel(x, y, color)
        placed.append({"x": x, "y": y, "color": color})
    if placed:
        inc = len(placed) if not user.get("god_mode") else 0
        users_col.update_one({"_id": user["_id"]}, {
            "$inc": {"pixels_left": -inc, "pixels_placed": len(placed), "pixels_placed_day": len(placed)},
            "$set": {"day_key": day_key()}
        })
        now = datetime.utcnow()
        lp = placed[-1]
        users_col.update_one({"_id": user["_id"]}, {"$set": {
            "last_seen": now,
            "last_pixel": {"x": lp["x"], "y": lp["y"], "color": lp["color"], "t": now.isoformat()}
        }})
        global last_remote_pixel
        last_remote_pixel = {"x": lp["x"], "y": lp["y"], "color": lp["color"], "username": user["username"], "t": now.isoformat()}
        await manager.broadcast({"type": "pixels", "pixels": placed, "username": user["username"], "t": now.isoformat()})
    left = user.get("pixels_left", 0) - (0 if user.get("god_mode") else len(placed))
    return {"ok": True, "placed": len(placed), "skipped": skipped, "pixels_left": left}

@app.get("/lookup")
async def lookup(x: int = Query(...), y: int = Query(...)):
    if pixels_col is None:
        return {"empty": True}
    px = pixels_col.find_one({"x": x, "y": y})
    zone = check_zone(x, y)
    zone_info = None
    if zone:
        zone_info = {"mod": zone.get("mod"), "expires_at": str(zone.get("expires_at"))}
    if not px:
        return {"empty": True, "zone": zone_info}
    info = {
        "display_name": px.get("display_name") or px.get("username"),
        "username": px.get("username"),
        "color": px.get("color"),
        "placed_at": str(px.get("placed_at", "")),
        "pixels_left": 0, "limit": 0, "medals": {}, "zone": zone_info
    }
    if px.get("user_id") and users_col is not None:
        try:
            u = users_col.find_one({"_id": ObjectId(px["user_id"])})
            if u:
                info.update(user_public(u))
                info["limit"] = get_pixel_limit(u)
                info["last_seen"] = str(u.get("last_seen") or "")
                info["pixels_placed"] = u.get("pixels_placed", 0)
        except Exception:
            pass
    return info


def _cache_key(x: int, y: int) -> str:
    return f"{x},{y}"

def ensure_pixel_cache():
    """Load all pixels from Mongo into RAM once (fast subsequent loads)."""
    global _pixel_cache, _pixel_cache_ready
    if _pixel_cache_ready and _pixel_cache is not None:
        return
    _pixel_cache = {}
    if pixels_col is not None:
        for doc in pixels_col.find({}, {"_id": 0, "x": 1, "y": 1, "color": 1}):
            try:
                _pixel_cache[_cache_key(int(doc["x"]), int(doc["y"]))] = doc["color"]
            except Exception:
                pass
    _pixel_cache_ready = True

def cache_set_pixel(x: int, y: int, color: str):
    global _pixel_cache
    ensure_pixel_cache()
    if _pixel_cache is not None:
        _pixel_cache[_cache_key(x, y)] = color

def cache_del_pixel(x: int, y: int):
    global _pixel_cache
    if _pixel_cache is not None:
        _pixel_cache.pop(_cache_key(x, y), None)

def cache_clear_region(x1, y1, x2, y2, only_color=None):
    global _pixel_cache
    if _pixel_cache is None:
        return
    to_del = []
    for k, col in _pixel_cache.items():
        try:
            xs, ys = k.split(",")
            x, y = int(xs), int(ys)
        except Exception:
            continue
        if x1 <= x <= x2 and y1 <= y <= y2:
            if only_color is None or col.lower() == only_color.lower():
                to_del.append(k)
    for k in to_del:
        del _pixel_cache[k]


@app.get("/canvas/chunk")
async def get_chunk(x1: int = 0, y1: int = 0, x2: int = 512, y2: int = 512):
    ensure_pixel_cache()
    x1, y1 = max(0, x1), max(0, y1)
    x2, y2 = min(max(x2, x1 + 1), CANVAS_SIZE), min(max(y2, y1 + 1), CANVAS_SIZE)
    out = []
    if _pixel_cache is None:
        return out
    for k, col in _pixel_cache.items():
        try:
            xs, ys = k.split(",")
            x, y = int(xs), int(ys)
        except Exception:
            continue
        if x1 <= x < x2 and y1 <= y < y2:
            out.append({"x": x, "y": y, "color": col})
    return out

@app.get("/canvas/all")
async def get_all_pixels():
    """Single payload of all painted pixels — used for fast map load."""
    ensure_pixel_cache()
    if not _pixel_cache:
        return {"pixels": [], "count": 0}
    # compact arrays: faster JSON + smaller than list of objects
    xs, ys, cs = [], [], []
    for k, col in _pixel_cache.items():
        try:
            a, b = k.split(",")
            xs.append(int(a)); ys.append(int(b)); cs.append(col)
        except Exception:
            pass
    return {"xs": xs, "ys": ys, "cs": cs, "count": len(xs)}

@app.get("/stats")
async def stats():
    total_pixels = pixels_col.count_documents({}) if pixels_col is not None else 0
    return {"online": online_count, "pixels": total_pixels}

@app.get("/leaderboard")
async def leaderboard(period: str = "daily"):
    if users_col is None:
        return []
    if period == "daily":
        cursor = users_col.find({"day_key": day_key()}).sort("pixels_placed_day", DESCENDING).limit(50)
        return [{"rank": i+1, "username": u["username"], "display_name": u.get("display_name") or u["username"],
                 "score": u.get("pixels_placed_day", 0), "avatar_color": u.get("avatar_color", 0)}
                for i, u in enumerate(cursor)]
    cursor = users_col.find({}).sort("pixels_placed", DESCENDING).limit(50)
    return [{"rank": i+1, "username": u["username"], "display_name": u.get("display_name") or u["username"],
             "score": u.get("pixels_placed", 0), "avatar_color": u.get("avatar_color", 0)}
            for i, u in enumerate(cursor)]

# ---------- REGEN AFK: +1 every 2 seconds = 0.5/s, or higher with rank ----------
@app.post("/regen")
async def regen(user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    if user.get("god_mode"):
        return {"ok": True, "added": 0, "pixels_left": user.get("pixels_left", 0), "limit": get_pixel_limit(user)}
    now = datetime.utcnow()
    last = user.get("last_regen") or now
    if isinstance(last, str):
        try:
            last = datetime.fromisoformat(last.replace("Z", ""))
        except Exception:
            last = now
    elapsed = max(0.0, (now - last).total_seconds())
    rate = rank_bonuses(user)["afk_rate"]  # default 0.5
    frac = float(user.get("pixel_frac") or 0) + elapsed * rate
    whole = int(frac)
    frac = frac - whole
    limit = get_pixel_limit(user)
    cur = int(user.get("pixels_left") or 0)
    add = min(whole, max(0, limit - cur))
    users_col.update_one({"_id": user["_id"]}, {"$set": {"last_regen": now, "pixel_frac": frac}, "$inc": {"pixels_left": add}})
    return {"ok": True, "added": add, "pixels_left": cur + add, "limit": limit}

# ---------- MOD ----------
@app.post("/mod/become")
async def become_mod(data: BecomeMod, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    if (data.code or "").strip() != MOD_CODE:
        raise HTTPException(403, "Wrong code")
    users_col.update_one({"_id": user["_id"]}, {"$set": {"is_mod": True}, "$inc": {"pixels_left": MAX_PIXELS_MOD_BONUS}})
    return {"ok": True}

@app.post("/mod/leave")
async def leave_mod(user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    users_col.update_one({"_id": user["_id"]}, {"$set": {"is_mod": False, "god_mode": False}})
    return {"ok": True}

@app.post("/mod/self_restrict")
async def self_restrict(data: ModAction, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    minutes = max(1, min(data.minutes or 2, 10))
    until = datetime.utcnow() + timedelta(minutes=minutes)
    users_col.update_one({"_id": user["_id"]}, {"$set": {
        "no_place_until": until, "no_place_reason": "Pencil speed limit", "no_place_by": "system"
    }})
    return {"ok": True, "until": until}

@app.post("/mod/god")
async def mod_god(user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    new_val = not user.get("god_mode", False)
    users_col.update_one({"_id": user["_id"]}, {"$set": {"god_mode": new_val}})
    return {"ok": True, "god_mode": new_val}

@app.post("/mod/ban")
async def mod_ban(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = data.target.strip().lstrip("@").lower()
    users_col.update_one({"username": target}, {"$set": {"banned": True, "ban_reason": data.reason or "", "banned_by": user["username"]}})
    return {"ok": True}

@app.post("/mod/unban")
async def mod_unban(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = data.target.strip().lstrip("@").lower()
    users_col.update_one({"username": target}, {"$set": {"banned": False}, "$unset": {"ban_reason": "", "banned_by": ""}})
    return {"ok": True}

@app.post("/mod/no_place")
async def mod_no_place(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = data.target.strip().lstrip("@").lower()
    minutes = max(1, min(data.minutes or 60, 2880))
    until = datetime.utcnow() + timedelta(minutes=minutes)
    users_col.update_one({"username": target}, {"$set": {
        "no_place_until": until, "no_place_reason": data.reason or "", "no_place_by": user["username"]
    }})
    return {"ok": True, "until": until}

@app.post("/mod/allow_place")
async def mod_allow_place(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = data.target.strip().lstrip("@").lower()
    users_col.update_one({"username": target}, {"$unset": {"no_place_until": "", "no_place_reason": "", "no_place_by": ""}})
    return {"ok": True}

@app.post("/mod/mute")
async def mod_mute(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = data.target.strip().lstrip("@").lower()
    until = datetime.utcnow() + timedelta(minutes=data.minutes or 30)
    users_col.update_one({"username": target}, {"$set": {"muted_until": until}})
    return {"ok": True}

@app.post("/mod/discord_medal")
async def mod_discord(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = data.target.strip().lstrip("@").lower()
    u = users_col.find_one({"username": target})
    if not u:
        raise HTTPException(404, "User not found")
    give = not u.get("discord_medal", False)
    if data.reason == "remove":
        give = False
    elif data.reason == "give":
        give = True
    upd = {"discord_medal": give}
    inc = {}
    if give and not u.get("discord_medal"):
        inc = {"pixels_left": MAX_PIXELS_DISCORD_BONUS}
    users_col.update_one({"username": target}, {"$set": upd, **({"$inc": inc} if inc else {})})
    return {"ok": True, "discord_medal": give}

@app.post("/mod/clear_water")
async def clear_water(data: ClearWater, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    if pixels_col is None:
        raise HTTPException(503, "DB not ready")
    if data.full:
        pixels_col.delete_many({})
        global _pixel_cache
        if _pixel_cache is not None:
            _pixel_cache.clear()
        await manager.broadcast({"type": "clear_all"})
        return {"ok": True, "msg": "Full clear"}
    x_min, x_max = min(data.x1, data.x2), max(data.x1, data.x2)
    y_min, y_max = min(data.y1, data.y2), max(data.y1, data.y2)
    q = {"x": {"$gte": x_min, "$lte": x_max}, "y": {"$gte": y_min, "$lte": y_max}}
    if data.color:
        q["color"] = norm_color(data.color)
    result = pixels_col.delete_many(q)
    cache_clear_region(x_min, y_min, x_max, y_max, only_color=None)
    await manager.broadcast({"type": "clear", "x1": x_min, "y1": y_min, "x2": x_max, "y2": y_max})
    return {"ok": True, "deleted": result.deleted_count}

@app.post("/mod/replace_color")
async def replace_color(data: ReplaceColor, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    if pixels_col is None:
        raise HTTPException(503, "DB not ready")
    fr, to = norm_color(data.from_color), norm_color(data.to_color)
    result = pixels_col.update_many({"color": fr}, {"$set": {"color": to}})
    ensure_pixel_cache()
    if _pixel_cache is not None:
        for k, col in list(_pixel_cache.items()):
            if col.lower() == fr.lower():
                _pixel_cache[k] = to
    await manager.broadcast({"type": "recolor", "from": fr, "to": to})
    return {"ok": True, "modified": result.modified_count}

@app.post("/mod/protect")
async def protect_zone(data: ProtectZone, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    x1, x2 = min(data.x1, data.x2), max(data.x1, data.x2)
    y1, y2 = min(data.y1, data.y2), max(data.y1, data.y2)
    if getattr(data, "forever", False) or data.minutes == 0:
        expires = datetime.utcnow() + timedelta(days=36500)  # ~100 years
    else:
        minutes = max(1, min(data.minutes, 10080))
        expires = datetime.utcnow() + timedelta(minutes=minutes)
    zones_col.insert_one({
        "x1": x1, "y1": y1, "x2": x2, "y2": y2,
        "mod": user["username"], "expires_at": expires, "created_at": datetime.utcnow()
    })
    await manager.broadcast({"type": "zone", "x1": x1, "y1": y1, "x2": x2, "y2": y2, "mod": user["username"], "expires_at": str(expires)})
    return {"ok": True, "expires_at": expires}


@app.post("/mod/zones_clear")
async def zones_clear(user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    if zones_col is None:
        return {"ok": True, "deleted": 0}
    res = zones_col.delete_many({})
    return {"ok": True, "deleted": res.deleted_count}

@app.get("/zones")
async def get_zones():
    if zones_col is None:
        return []
    now = datetime.utcnow()
    zones_col.delete_many({"expires_at": {"$lt": now}})
    return [{"x1": z["x1"], "y1": z["y1"], "x2": z["x2"], "y2": z["y2"], "mod": z["mod"], "expires_at": str(z["expires_at"])}
            for z in zones_col.find({"expires_at": {"$gt": now}})]

# ---------- EARN: animal OR math 50/50 ----------
ANIMALS = ["cat","dog","tiger","bear","antelope","elephant","hippo","crocodile","human","mosquito","pig","fox","wolf","rabbit","owl","snake","deer","lion","panda","koala"]

@app.get("/earn/start")
async def earn_start(user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    limit = get_pixel_limit(user)
    cur = int(user.get("pixels_left") or 0)
    if cur >= limit:
        raise HTTPException(400, "Limit reached — spend pixels first")
    streak = int(user.get("earn_streak") or 0)
    sid = str(random.randint(100000, 999999))
    correct = random.choice(ANIMALS)
    options = random.sample([a for a in ANIMALS if a != correct], 8) + [correct]
    random.shuffle(options)
    earn_sessions[sid] = {"type": "animal", "correct": correct}
    if len(earn_sessions) > 20000:
        earn_sessions.clear()
    return {"session_id": sid, "mode": "animal", "options": options, "animal": correct, "cooldown": 15}

@app.post("/earn/check")
async def earn_check(data: EarnCheck, session_id: str = Query(...), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    sess = earn_sessions.pop(session_id, None)
    if not sess:
        raise HTTPException(400, "Session expired")
    correct = sess["correct"]
    if str(data.answer).strip().lower() != str(correct).strip().lower():
        users_col.update_one({"_id": user["_id"]}, {"$set": {"earn_streak": 0}})
        return {"ok": False, "next_cooldown": 15, "correct": correct}
    streak = int(user.get("earn_streak") or 0) + 1
    base = min(20, int(15 + (streak - 1) * 0.05))
    bonus = rank_bonuses(user)["earn_bonus"]
    add = base + bonus
    limit = get_pixel_limit(user)
    cur = int(user.get("pixels_left") or 0)
    add = min(add, max(0, limit - cur))
    if add <= 0:
        return {"ok": False, "next_cooldown": 15, "msg": "Limit reached"}
    upd = {"$inc": {"pixels_left": add}, "$set": {"earn_streak": streak}}
    if streak >= 5:
        upd["$set"]["streak_medal"] = True
    users_col.update_one({"_id": user["_id"]}, upd)
    return {"ok": True, "added": add, "streak": streak, "next_cooldown": 15}


class TemplateSave(BaseModel):
    templates: list  # [{x,y,w,h,data_url}] max 5

@app.get("/templates")
async def get_templates(user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    return user.get("templates") or []

@app.post("/templates")
async def save_templates(data: TemplateSave, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    tpls = data.templates[:5]
    # limit size ~1.5MB each roughly by truncating huge
    clean = []
    for t in tpls:
        du = (t.get("data_url") or "")[:2_000_000]
        clean.append({"x": int(t.get("x") or 0), "y": int(t.get("y") or 0),
                      "w": int(t.get("w") or 0), "h": int(t.get("h") or 0),
                      "label": str(t.get("label") or "")[:20], "data_url": du})
    users_col.update_one({"_id": user["_id"]}, {"$set": {"templates": clean}})
    return {"ok": True, "count": len(clean)}


class PublicTemplate(BaseModel):
    label: str
    x: int
    y: int
    w: int
    h: int
    data_url: str

@app.post("/templates/publish")
async def publish_template(data: PublicTemplate, user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    if public_tpl_col is None:
        raise HTTPException(503)
    label = (data.label or "template")[:20]
    du = (data.data_url or "")[:2_000_000]
    public_tpl_col.insert_one({
        "label": label, "x": data.x, "y": data.y, "w": data.w, "h": data.h,
        "data_url": du, "author": user["username"],
        "author_name": user.get("display_name") or user["username"],
        "created": datetime.utcnow(), "installs": 0
    })
    return {"ok": True}

@app.get("/templates/public")
async def list_public_templates():
    if public_tpl_col is None:
        return []
    out = []
    for t in public_tpl_col.find().sort("created", -1).limit(50):
        out.append({
            "id": str(t["_id"]), "label": t.get("label"), "x": t["x"], "y": t["y"],
            "w": t["w"], "h": t["h"], "data_url": t.get("data_url"),
            "author": t.get("author"), "author_name": t.get("author_name"),
            "created": t.get("created"), "installs": t.get("installs", 0)
        })
    return out

@app.post("/templates/install")
async def install_public(tid: str = Body(..., embed=True)):
    if public_tpl_col is None:
        raise HTTPException(503)
    try:
        public_tpl_col.update_one({"_id": ObjectId(tid)}, {"$inc": {"installs": 1}})
    except Exception:
        pass
    return {"ok": True}

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
        res = clans_col.insert_one({"name": name, "tag": tag, "leader": user["username"],
            "members": [user["username"]], "created_at": datetime.utcnow()})
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
    bans = clan.get("bans") or {}
    ban = bans.get(user["username"])
    if ban:
        until = ban.get("until")
        if until and until > datetime.utcnow():
            raise HTTPException(403, f"Banned until {until} by @{ban.get('by')}")
    clans_col.update_one({"_id": clan["_id"]}, {"$addToSet": {"members": user["username"]}})
    users_col.update_one({"_id": user["_id"]}, {"$set": {"clan_id": str(clan["_id"]), "clan_tag": tag}})
    return {"ok": True}


@app.post("/clan/kick")
async def clan_kick(target: str = Body(..., embed=True), user=Depends(get_current_user)):
    if not user or not user.get("clan_id"):
        raise HTTPException(400, "No clan")
    clan = clans_col.find_one({"_id": ObjectId(user["clan_id"])})
    if not clan or clan.get("leader") != user["username"]:
        raise HTTPException(403, "Leader only")
    t = target.strip().lstrip("@").lower()
    if t == user["username"]:
        raise HTTPException(400, "Cannot kick yourself")
    clans_col.update_one({"_id": clan["_id"]}, {"$pull": {"members": t}})
    users_col.update_one({"username": t}, {"$unset": {"clan_id": "", "clan_tag": ""}})
    return {"ok": True}

@app.post("/clan/ban")
async def clan_ban(data: dict = Body(...), user=Depends(get_current_user)):
    target = data.get("target", "")
    minutes = int(data.get("minutes") or 60)
    if not user or not user.get("clan_id"):
        raise HTTPException(400, "No clan")
    clan = clans_col.find_one({"_id": ObjectId(user["clan_id"])})
    if not clan or clan.get("leader") != user["username"]:
        raise HTTPException(403, "Leader only")
    t = target.strip().lstrip("@").lower()
    until = datetime.utcnow() + timedelta(minutes=max(1, min(minutes, 10080)))
    bans = clan.get("bans") or {}
    bans[t] = {"until": until, "by": user["username"], "by_name": user.get("display_name") or user["username"]}
    clans_col.update_one({"_id": clan["_id"]}, {"$set": {"bans": bans}, "$pull": {"members": t}})
    users_col.update_one({"username": t}, {"$unset": {"clan_id": "", "clan_tag": ""}})
    return {"ok": True}

@app.post("/clan/leave")
async def clan_leave(user=Depends(get_current_user)):
    if not user or not user.get("clan_id"):
        raise HTTPException(400, "No clan")
    clan = clans_col.find_one({"_id": ObjectId(user["clan_id"])})
    if not clan:
        users_col.update_one({"_id": user["_id"]}, {"$unset": {"clan_id": "", "clan_tag": ""}})
        return {"ok": True}
    if clan.get("leader") == user["username"]:
        # kick all
        for m in clan.get("members", []):
            users_col.update_one({"username": m}, {"$unset": {"clan_id": "", "clan_tag": ""}})
        clans_col.delete_one({"_id": clan["_id"]})
    else:
        clans_col.update_one({"_id": clan["_id"]}, {"$pull": {"members": user["username"]}})
        users_col.update_one({"_id": user["_id"]}, {"$unset": {"clan_id": "", "clan_tag": ""}})
    return {"ok": True}

@app.get("/clan/info")
async def clan_info(user=Depends(get_current_user)):
    if not user or not user.get("clan_id"):
        return {"empty": True}
    clan = clans_col.find_one({"_id": ObjectId(user["clan_id"])})
    if not clan:
        return {"empty": True}
    members = []
    for un in clan.get("members", []):
        u = users_col.find_one({"username": un})
        if u:
            members.append({
                "username": un,
                "display_name": u.get("display_name") or un,
                "is_leader": un == clan.get("leader"),
                "pixels_placed": u.get("pixels_placed", 0),
                "pixels_left": u.get("pixels_left", 0),
                "medals": {
                    "account": True,
                    "discord": bool(u.get("discord_medal")),
                    "mod": bool(u.get("is_mod")),
                    "clan": True,
                    "streak": bool(u.get("streak_medal")),
                }
            })
    return {"name": clan["name"], "tag": clan["tag"], "leader": clan["leader"], "members": members}


@app.get("/clans")
async def list_clans():
    if clans_col is None:
        return []
    out = []
    for c in clans_col.find({}, {"name": 1, "tag": 1, "leader": 1, "members": 1}).limit(50):
        out.append({"id": str(c["_id"]), "name": c["name"], "tag": c.get("tag", ""), "leader": c["leader"], "members_count": len(c.get("members", []))})
    return out

# ---------- CHAT (auto-delete 1 hour) ----------
@app.post("/chat")
async def send_chat(message: str = Body(..., embed=True), channel: str = Body("en"), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    muted = user.get("muted_until")
    if muted and muted > datetime.utcnow():
        raise HTTPException(403, "Muted")
    if len(message) > 200:
        raise HTTPException(400, "Too long")
    if channel not in ("global", "en", "ru", "ar", "tr", "clan"):
        channel = "en"
    doc = {
        "username": user["username"],
        "display_name": user.get("display_name") or user["username"],
        "message": message[:200],
        "channel": channel,
        "is_mod": user.get("is_mod", False),
        "avatar_color": user.get("avatar_color", 0),
        "created_at": datetime.utcnow()
    }
    if chats_col is not None:
        chats_col.insert_one(doc)
        keep = 200 if channel == "clan" else 100
        extra = list(chats_col.find({"channel": channel}).sort("created_at", DESCENDING).skip(keep))
        if extra:
            chats_col.delete_many({"_id": {"$in": [e["_id"] for e in extra]}})
    await manager.broadcast({"type": "chat", "username": doc["username"], "display_name": doc["display_name"],
        "message": doc["message"], "channel": channel, "is_mod": doc["is_mod"],
        "avatar_color": doc["avatar_color"], "created_at": str(doc["created_at"])})
    return {"ok": True}

@app.get("/chat/history")
async def chat_history(channel: str = "en", limit: int = 100):
    if chats_col is None:
        return []
    lim = 200 if channel == "clan" else 100
    cursor = chats_col.find({"channel": channel}).sort("created_at", DESCENDING).limit(min(limit, lim))
    return [{"username": c["username"], "display_name": c.get("display_name"), "message": c["message"],
             "is_mod": c.get("is_mod", False), "avatar_color": c.get("avatar_color", 0),
             "created_at": str(c["created_at"])} for c in cursor][::-1]


@app.get("/radar")
async def get_radar(user=Depends(get_current_user)):
    """Last pixel placed by someone else (or anyone if guest)."""
    global last_remote_pixel
    if last_remote_pixel:
        # if same user placed last, try find another from recent? keep global last
        if user and last_remote_pixel.get("username") == user.get("username"):
            # still return it but client may prefer "other"
            pass
        return last_remote_pixel
    return {}

@app.get("/mod/recent_online")
async def mod_recent_online(user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    # from online_users + DB last_seen
    seen = {}
    out = []
    for info in online_users.values():
        un = info.get("username") or "guest"
        if un in seen or un == "guest":
            continue
        seen[un] = True
        out.append({"username": un, "display_name": info.get("display_name") or un, "joined": info.get("joined"), "online": True})
    if users_col is not None:
        for u in users_col.find({"last_seen": {"$exists": True}}).sort("last_seen", -1).limit(30):
            un = u.get("username")
            if un in seen:
                continue
            seen[un] = True
            out.append({
                "username": un,
                "display_name": u.get("display_name") or un,
                "joined": str(u.get("last_seen")),
                "online": False,
                "last_pixel": u.get("last_pixel"),
            })
    return out[:40]

@app.post("/mod/grant_youtube")
async def grant_youtube(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = (data.target or "").strip().lstrip("@").lower()
    t = users_col.find_one({"username": target})
    if not t:
        raise HTTPException(404, "User not found")
    users_col.update_one({"_id": t["_id"]}, {"$set": {"youtube_medal": True}})
    return {"ok": True}

@app.post("/mod/revoke_youtube")
async def revoke_youtube(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = (data.target or "").strip().lstrip("@").lower()
    users_col.update_one({"username": target}, {"$set": {"youtube_medal": False}})
    return {"ok": True}

# ---- NEWS ----
class NewsPost(BaseModel):
    title: str
    body: str
    image_url: Optional[str] = None

@app.get("/news")
async def list_news():
    if news_col is None:
        return []
    out = []
    for n in news_col.find().sort("created", -1).limit(30):
        out.append({
            "id": str(n["_id"]),
            "title": n.get("title"),
            "body": n.get("body"),
            "image_url": n.get("image_url"),
            "author": n.get("author"),
            "created": str(n.get("created")),
            "likes": n.get("likes", 0),
            "dislikes": n.get("dislikes", 0),
            "comments": n.get("comments", [])[-50:],
        })
    return out

@app.post("/news")
async def post_news(data: NewsPost, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    if news_col is None:
        raise HTTPException(503)
    news_col.insert_one({
        "title": (data.title or "")[:80],
        "body": (data.body or "")[:2000],
        "image_url": (data.image_url or "")[:500] or None,
        "author": user["username"],
        "created": datetime.utcnow(),
        "likes": 0, "dislikes": 0, "comments": [],
        "liked_by": [], "disliked_by": [],
    })
    return {"ok": True}

@app.post("/news/react")
async def news_react(nid: str = Body(...), reaction: str = Body(...), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    if news_col is None:
        raise HTTPException(503)
    try:
        oid = ObjectId(nid)
    except Exception:
        raise HTTPException(400)
    n = news_col.find_one({"_id": oid})
    if not n:
        raise HTTPException(404)
    un = user["username"]
    liked = n.get("liked_by") or []
    disliked = n.get("disliked_by") or []
    if reaction == "like":
        if un in liked:
            liked = [x for x in liked if x != un]
        else:
            liked = liked + [un]
            disliked = [x for x in disliked if x != un]
    elif reaction == "dislike":
        if un in disliked:
            disliked = [x for x in disliked if x != un]
        else:
            disliked = disliked + [un]
            liked = [x for x in liked if x != un]
    news_col.update_one({"_id": oid}, {"$set": {
        "liked_by": liked, "disliked_by": disliked,
        "likes": len(liked), "dislikes": len(disliked)
    }})
    return {"ok": True, "likes": len(liked), "dislikes": len(disliked)}

@app.post("/news/comment")
async def news_comment(nid: str = Body(...), text: str = Body(...), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    if news_col is None:
        raise HTTPException(503)
    text = (text or "").strip()[:300]
    if not text:
        raise HTTPException(400)
    try:
        oid = ObjectId(nid)
    except Exception:
        raise HTTPException(400)
    c = {
        "username": user["username"],
        "display_name": user.get("display_name") or user["username"],
        "text": text,
        "created": datetime.utcnow().isoformat(),
    }
    news_col.update_one({"_id": oid}, {"$push": {"comments": {"$each": [c], "$slice": -50}}})
    return {"ok": True}

# identify updates last_seen

# ---------- WS / STATIC ----------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text("pong")
                continue
            try:
                msg = json.loads(data)
                if msg.get("type") == "identify" and msg.get("token"):
                    try:
                        payload = jwt.decode(msg["token"], SECRET_KEY, algorithms=[ALGORITHM])
                        uname = payload.get("sub")
                        if uname and users_col is not None:
                            u = users_col.find_one({"username": uname})
                            if u:
                                manager.identify(ws, u["username"], u.get("display_name") or u["username"])
                    except Exception:
                        pass
            except Exception:
                pass
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)

@app.get("/online")
async def list_online():
    seen = {}
    out = []
    for info in online_users.values():
        key = info.get("username") or "guest"
        if key in seen:
            continue
        seen[key] = True
        out.append(info)
    return out

@app.get("/")
async def root():
    p = _find_file("index.html")
    if p:
        return FileResponse(p, media_type="text/html")
    return HTMLResponse("<h2>index.html not found</h2>", status_code=404)

@app.get("/world_map.png")
async def world_map():
    p = _find_file("world_map.png")
    if p:
        return FileResponse(p, media_type="image/png")
    raise HTTPException(404, "world_map.png not found")

@app.get("/health")
async def health():
    base = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    listings = {}
    for label, path in [("base", base), ("cwd", cwd), ("src", "/opt/render/project/src")]:
        try:
            listings[label] = {"path": path, "files": os.listdir(path)[:40]}
        except Exception as e:
            listings[label] = {"path": path, "error": str(e)}
    return {
        "status": "ok",
        "mongo": users_col is not None,
        "index_html": _find_file("index.html"),
        "world_map": _find_file("world_map.png"),
        "listings": listings,
        "online": online_count,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
