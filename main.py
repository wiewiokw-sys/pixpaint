
import os
import re
import random
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
import asyncio

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
last_remote_pixel: Optional[dict] = None  # {x,y,color,username,t} — kept for compat
_recent_pixels: List[dict] = []  # [{x,y,color,username,t,ts}] TTL 120s
RECENT_PIXEL_TTL = 120  # seconds (radar keeps info 2 min)
# Anti-lag: per-user place rate limit (even god mode) ~80 px/s
_place_rate: Dict[str, list] = {}  # username -> [timestamps]
PLACE_RATE_WINDOW = 1.0  # seconds
PLACE_RATE_MAX = 80  # max pixels accepted per window (batch counts as N)

def _check_place_rate(username: str, n: int = 1) -> int:
    """Return how many of n pixels are allowed under the rate limit. 0 = none."""
    now = datetime.utcnow().timestamp()
    arr = _place_rate.get(username) or []
    arr = [t for t in arr if now - t < PLACE_RATE_WINDOW]
    room = max(0, PLACE_RATE_MAX - len(arr))
    if room <= 0:
        _place_rate[username] = arr
        return 0
    take = min(n, room)
    arr.extend([now] * take)
    _place_rate[username] = arr[-PLACE_RATE_MAX:]
    return take

def _compact_px(placed: list) -> dict:
    """Compact WS payload: {type:'px', p:[[x,y,c],...]} — no username, less bytes."""
    return {"type": "px", "p": [[p["x"], p["y"], p["color"]] for p in placed]}

def _prune_recent_pixels():
    global _recent_pixels, last_remote_pixel
    now = datetime.utcnow().timestamp()
    _recent_pixels = [r for r in _recent_pixels if now - r.get("ts", 0) < RECENT_PIXEL_TTL]
    if _recent_pixels:
        last_remote_pixel = {k: _recent_pixels[-1][k] for k in ("x", "y", "color", "username", "t") if k in _recent_pixels[-1]}
    else:
        last_remote_pixel = None

def _push_recent_pixel(x, y, color, username):
    global _recent_pixels, last_remote_pixel
    now = datetime.utcnow()
    entry = {"x": x, "y": y, "color": color, "username": username, "t": now.isoformat(), "ts": now.timestamp()}
    _recent_pixels.append(entry)
    if len(_recent_pixels) > 200:
        _recent_pixels = _recent_pixels[-100:]
    _prune_recent_pixels()
    last_remote_pixel = {"x": x, "y": y, "color": color, "username": username, "t": entry["t"]}

news_col = None
_pixel_cache: Optional[Dict[str, str]] = None  # "x,y" -> color
_pixel_cache_ready = False
_pixel_arrays: Optional[Dict[str, Any]] = None  # prebuilt {xs,ys,cs,count} for /canvas/all
_pixel_arrays_dirty = True
# Global server events
# mode: None|night|haos|won|blue|yellow|red|green|select
# bonuses: earn_mul, earn_add, afk_mul, afk_add, stock_add, opacity
_global_event: Dict[str, Any] = {
    "mode": None, "ends_at": None, "overlay": False, "set_by": None,
    "opacity": 0.8, "color": None,
    "earn_mul": 1.0, "earn_add": 0.0, "afk_mul": 1.0, "afk_add": 0.0, "stock_add": 0,
    "select": None,  # {options: {1:{...},...}, ends_choice, starts_bonus, duration_m}
}
# per-user temporary bonuses from select events: username -> {earn_add, afk_add, stock_add, ends_at}
_user_event_bonus: Dict[str, dict] = {}
# Broadcast window SMS for online + late joiners
_window_sms: Optional[dict] = None  # {text, from, created}
# clan pixel pools: clan_id -> int
_clan_pools: Dict[str, int] = {}

def get_global_event() -> dict:
    global _global_event
    ends = _global_event.get("ends_at")
    if ends and isinstance(ends, datetime) and datetime.utcnow() >= ends:
        _global_event = {
            "mode": None, "ends_at": None, "overlay": False, "set_by": None,
            "opacity": 0.8, "color": None,
            "earn_mul": 1.0, "earn_add": 0.0, "afk_mul": 1.0, "afk_add": 0.0, "stock_add": 0,
            "select": None,
        }
    return dict(_global_event)

def get_user_event_bonus(username: str) -> dict:
    b = _user_event_bonus.get(username)
    if not b:
        return {}
    ends = b.get("ends_at")
    if ends and isinstance(ends, datetime) and datetime.utcnow() >= ends:
        _user_event_bonus.pop(username, None)
        return {}
    return b

def event_afk_multiplier() -> float:
    ev = get_global_event()
    m = ev.get("mode")
    if m == "night":
        return 1 / 3
    if m == "haos":
        return 2.0
    if m == "won":
        return 1.2
    mul = float(ev.get("afk_mul") or 1.0)
    return mul

def event_afk_add() -> float:
    return float(get_global_event().get("afk_add") or 0.0)

def event_earn_multiplier() -> float:
    ev = get_global_event()
    m = ev.get("mode")
    if m in ("night", "haos", "won"):
        return event_afk_multiplier()
    return float(ev.get("earn_mul") or 1.0)

def event_earn_add() -> float:
    return float(get_global_event().get("earn_add") or 0.0)

def event_stock_add() -> int:
    return int(get_global_event().get("stock_add") or 0)

def parse_bonus_token(tok: str) -> dict:
    """Parse EARN(×2) EARN(+3) EARNAFK(÷2) STOCK(+200) etc."""
    out = {}
    tok = tok.strip()
    m = re.match(r"^(EARN|EARNAFK|STOCK)\((.+)\)$", tok, re.I)
    if not m:
        return out
    kind, val = m.group(1).upper(), m.group(2).strip()
    if kind == "STOCK":
        try:
            out["stock_add"] = int(float(val.replace("+", "")))
        except Exception:
            pass
        return out
    key_mul = "earn_mul" if kind == "EARN" else "afk_mul"
    key_add = "earn_add" if kind == "EARN" else "afk_add"
    if val.startswith("×") or val.startswith("x") or val.startswith("X") or val.startswith("*"):
        try:
            out[key_mul] = float(val.lstrip("×xX*"))
        except Exception:
            pass
    elif val.startswith("÷") or val.startswith("/"):
        try:
            d = float(val.lstrip("÷/"))
            if d:
                out[key_mul] = 1.0 / d
        except Exception:
            pass
    else:
        try:
            out[key_add] = float(val.replace("+", ""))
        except Exception:
            pass
    return out

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
    global client, db, users_col, pixels_col, clans_col, chats_col, zones_col, public_tpl_col, news_col
    if not MONGO_URI:
        print("ERROR: MONGO_URI is not set — all data will be empty!")
    else:
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
            client.admin.command("ping")
            db = client.get_database("pixpaint")
            users_col = db["users"]
            pixels_col = db["pixels"]
            clans_col = db["clans"]
            chats_col = db["chats"]
            zones_col = db["zones"]
            public_tpl_col = db["public_templates"]
            news_col = db["news"]
            try:
                pixels_col.create_index([("x", ASCENDING), ("y", ASCENDING)], unique=True)
                users_col.create_index("username", unique=True)
                clans_col.create_index("tag", unique=True)
                chats_col.create_index("created_at")
                zones_col.create_index("expires_at")
            except Exception as e:
                print("Index warning:", e)
            print("MongoDB connected OK, pixels=", pixels_col.estimated_document_count())
            # warm pixel cache in background so first visitor isn't blocked
            try:
                import threading
                threading.Thread(target=ensure_pixel_cache, daemon=True).start()
            except Exception as e:
                print("cache warm fail:", e)
        except Exception as e:
            print("MongoDB FAILED:", e)
            client = db = users_col = pixels_col = clans_col = chats_col = zones_col = public_tpl_col = news_col = None
    yield
    if client:
        try:
            client.close()
        except Exception:
            pass

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
    # god_mode: same stock/limit as normal, but places are free (see /place)
    limit = MAX_PIXELS_AUTH + ACCOUNT_MEDAL_BONUS
    if user.get("discord_medal"):
        limit += MAX_PIXELS_DISCORD_BONUS
    if user.get("is_mod"):
        limit += MAX_PIXELS_MOD_BONUS
    if user.get("streak_medal"):
        limit += STREAK_MEDAL_BONUS
    if user.get("youtube_medal"):
        limit += 15
    if user.get("pixels_2m_medal") or user.get("pixels_placed", 0) >= 2_000_000:
        limit += 10
    # Twink accounts: 1/3 limit
    if user.get("is_twink"):
        # base calc continues then divide
        pass
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
    # global event multiplier (night / haos / won / color events)
    ev = get_global_event()
    if ev.get("mode") == "night":
        limit = max(1, int(limit / 3))
    elif ev.get("mode") == "haos":
        limit = int(limit * 2)
    elif ev.get("mode") == "won":
        limit = int(limit * 1.2)
    limit += event_stock_add()
    ub = get_user_event_bonus(user.get("username") or "")
    limit += int(ub.get("stock_add") or 0)
    if user.get("is_twink"):
        limit = max(1, int(limit / 3))
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
        "is_twink": bool(user.get("is_twink")),
        "twink_slot": user.get("twink_slot"),
        "parent_username": user.get("parent_username"),
        "muted_until": str(user.get("muted_until") or ""),
        "muted_by": user.get("muted_by"),
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
    old_username = user["username"]
    old_display = user.get("display_name") or old_username
    upd = {}
    if data.display_name is not None:
        dn = data.display_name.strip()
        if not dn or len(dn) > 32:
            raise HTTPException(400, "Bad display name")
        upd["display_name"] = dn
    if data.username is not None:
        un = data.username.strip().lstrip("@").lower()
        un = re.sub(r"[^a-z0-9_]", "", un)
        if len(un) < 3 or len(un) > 20:
            raise HTTPException(400, "Username 3-20 (a-z, 0-9, _)")
        if un != old_username and users_col.find_one({"username": un}):
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
    new_username = upd.get("username", old_username)
    new_display = upd.get("display_name", old_display)

    # Propagate name changes to related collections (chat, pixels, templates, news, clans)
    try:
        name_set = {}
        if "username" in upd:
            name_set["username"] = new_username
        if "display_name" in upd or "username" in upd:
            name_set["display_name"] = new_display
        if name_set and pixels_col is not None:
            pixels_col.update_many(
                {"$or": [{"username": old_username}, {"user_id": str(user["_id"])}]},
                {"$set": name_set}
            )
        if name_set and chats_col is not None:
            chat_set = {}
            if "username" in upd:
                chat_set["username"] = new_username
            if "display_name" in upd or "username" in upd:
                chat_set["display_name"] = new_display
            if chat_set:
                chats_col.update_many({"username": old_username}, {"$set": chat_set})
        if public_tpl_col is not None and ("username" in upd or "display_name" in upd):
            tpl_set = {}
            if "username" in upd:
                tpl_set["author"] = new_username
            if "display_name" in upd or "username" in upd:
                tpl_set["author_name"] = new_display
            if tpl_set:
                public_tpl_col.update_many({"author": old_username}, {"$set": tpl_set})
        if news_col is not None and ("username" in upd or "display_name" in upd):
            news_col.update_many({"author": old_username}, {"$set": {"author": new_username}})
            # comments inside news
            for n in news_col.find({"comments.username": old_username}):
                comments = n.get("comments") or []
                changed = False
                for c in comments:
                    if c.get("username") == old_username:
                        c["username"] = new_username
                        c["display_name"] = new_display
                        changed = True
                if changed:
                    news_col.update_one({"_id": n["_id"]}, {"$set": {"comments": comments}})
        if clans_col is not None and "username" in upd:
            # leader / members lists
            for clan in clans_col.find({"$or": [{"leader": old_username}, {"members": old_username}]}):
                members = list(clan.get("members") or [])
                members = [new_username if m == old_username else m for m in members]
                leader = new_username if clan.get("leader") == old_username else clan.get("leader")
                clans_col.update_one({"_id": clan["_id"]}, {"$set": {"members": members, "leader": leader}})
            users_col.update_many({"clan_tag": {"$exists": True}}, {"$set": {}})  # no-op keep
            # update clan_tag field on user already has new username via main update
        if users_col is not None and "username" in upd:
            # clan bans that store usernames
            pass
    except Exception as e:
        print("profile cascade warning:", e)

    # Always issue fresh token so JWT sub matches new username
    token = create_access_token({"sub": new_username})
    fresh = users_col.find_one({"_id": user["_id"]})
    return {
        "ok": True,
        "access_token": token,
        "token_type": "bearer",
        "user": {**user_public(fresh), "limit": get_pixel_limit(fresh)},
        "username_changed": "username" in upd,
        "login_as": new_username,
    }

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
    ERASE_COLOR = "#ff00ea"
    is_erase = (color == ERASE_COLOR and user.get("is_mod"))
    zone = check_zone(x, y)
    if zone and not is_erase:
        raise HTTPException(403, f"Protected by @{zone.get('mod','mod')} until {zone.get('expires_at')}")
    if is_erase:
        # Mod erase: delete pixel at position
        if pixels_col is not None:
            pixels_col.delete_one({"x": x, "y": y})
            cache_del_pixel(x, y)
        now = datetime.utcnow()
        users_col.update_one({"_id": user["_id"]}, {"$set": {"last_seen": now}})
        await manager.broadcast({"type": "erase", "x": x, "y": y, "username": user["username"], "t": now.isoformat()})
        return {"ok": True, "erased": True, "pixels_left": user.get("pixels_left", 0)}
    # same color = free
    if pixels_col is not None:
        existing = pixels_col.find_one({"x": x, "y": y})
        if existing and norm_color(existing.get("color", "")) == color:
            return {"ok": True, "pixels_left": user.get("pixels_left", 0), "skipped": True}
    if not user.get("god_mode") and user.get("pixels_left", 0) <= 0:
        raise HTTPException(400, "No pixels left")
    # rate limit even for god / multi-account (~80 px/s)
    if _check_place_rate(user["username"], 1) < 1:
        raise HTTPException(429, "Rate limit")
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
    _push_recent_pixel(x, y, color, user["username"])
    await manager.broadcast(_compact_px([{"x": x, "y": y, "color": color}]))
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
    # rate limit even for god — trim batch to available room
    room = _check_place_rate(user["username"], len(pts))
    if room <= 0:
        raise HTTPException(429, "Rate limit")
    pts = pts[:room]
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
        _push_recent_pixel(lp["x"], lp["y"], lp["color"], user["username"])
        await manager.broadcast(_compact_px(placed))
    left = user.get("pixels_left", 0) - (0 if user.get("god_mode") else len(placed))
    return {"ok": True, "placed": len(placed), "skipped": skipped, "pixels_left": left}

@app.get("/lookup")
async def lookup(x: int = Query(...), y: int = Query(...)):
    # Lookup disabled — no pixel owner info exposed
    return {"empty": True, "disabled": True}


def _cache_key(x: int, y: int) -> str:
    return f"{x},{y}"

def _rebuild_pixel_arrays():
    """Rebuild compact arrays for /canvas/all from _pixel_cache."""
    global _pixel_arrays, _pixel_arrays_dirty
    xs, ys, cs = [], [], []
    if _pixel_cache:
        for k, col in _pixel_cache.items():
            try:
                a, b = k.split(",")
                xs.append(int(a)); ys.append(int(b)); cs.append(col)
            except Exception:
                pass
    _pixel_arrays = {"xs": xs, "ys": ys, "cs": cs, "count": len(xs)}
    _pixel_arrays_dirty = False

def ensure_pixel_cache():
    """Load all pixels from Mongo into RAM once (fast subsequent loads)."""
    global _pixel_cache, _pixel_cache_ready, _pixel_arrays_dirty
    if _pixel_cache_ready and _pixel_cache is not None:
        return
    _pixel_cache = {}
    if pixels_col is not None:
        try:
            for doc in pixels_col.find({}, {"_id": 0, "x": 1, "y": 1, "color": 1}).batch_size(10000):
                try:
                    _pixel_cache[_cache_key(int(doc["x"]), int(doc["y"]))] = doc["color"]
                except Exception:
                    pass
            print("Pixel cache loaded:", len(_pixel_cache))
        except Exception as e:
            print("Pixel cache load error:", e)
            _pixel_cache = {}
            _pixel_cache_ready = False
            return
    _pixel_cache_ready = True
    _pixel_arrays_dirty = True
    _rebuild_pixel_arrays()

def cache_set_pixel(x: int, y: int, color: str):
    global _pixel_cache, _pixel_arrays, _pixel_arrays_dirty
    ensure_pixel_cache()
    if _pixel_cache is not None:
        key = _cache_key(x, y)
        prev = _pixel_cache.get(key)
        _pixel_cache[key] = color
        # incremental update of arrays when possible
        if _pixel_arrays is not None and prev is None:
            _pixel_arrays["xs"].append(x)
            _pixel_arrays["ys"].append(y)
            _pixel_arrays["cs"].append(color)
            _pixel_arrays["count"] = len(_pixel_arrays["xs"])
        elif _pixel_arrays is not None and prev != color:
            # color change — mark dirty (rare); next /canvas/all rebuilds
            _pixel_arrays_dirty = True
        else:
            _pixel_arrays_dirty = True

def cache_del_pixel(x: int, y: int):
    global _pixel_cache, _pixel_arrays_dirty
    if _pixel_cache is not None:
        _pixel_cache.pop(_cache_key(x, y), None)
        _pixel_arrays_dirty = True

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
    x1, y1 = max(0, int(x1)), max(0, int(y1))
    x2, y2 = min(max(int(x2), x1 + 1), CANVAS_SIZE), min(max(int(y2), y1 + 1), CANVAS_SIZE)
    def _load_chunk():
        # Prefer RAM cache (fast); fall back to Mongo
        ensure_pixel_cache()
        out = []
        if _pixel_cache:
            for k, col in _pixel_cache.items():
                try:
                    xs, ys = k.split(",")
                    x, y = int(xs), int(ys)
                except Exception:
                    continue
                if x1 <= x < x2 and y1 <= y < y2:
                    out.append({"x": x, "y": y, "color": col})
            return out
        if pixels_col is not None:
            try:
                return list(pixels_col.find(
                    {"x": {"$gte": x1, "$lt": x2}, "y": {"$gte": y1, "$lt": y2}},
                    {"_id": 0, "x": 1, "y": 1, "color": 1}
                ).limit(200000))
            except Exception as e:
                print("chunk mongo err", e)
        return []
    return await asyncio.to_thread(_load_chunk)

@app.get("/canvas/all")
async def get_all_pixels():
    """Single payload of all painted pixels — used for fast map load.
    Heavy Mongo load runs in a thread so the event loop (chat/place) stays responsive.
    """
    global _pixel_cache_ready, _pixel_arrays_dirty
    def _load():
        ensure_pixel_cache()
        if not _pixel_cache and pixels_col is not None:
            global _pixel_cache_ready
            _pixel_cache_ready = False
            ensure_pixel_cache()
        if _pixel_arrays_dirty or _pixel_arrays is None:
            _rebuild_pixel_arrays()
        return _pixel_arrays or {"xs": [], "ys": [], "cs": [], "count": 0}
    return await asyncio.to_thread(_load)

@app.get("/stats")
async def stats():
    total_pixels = pixels_col.count_documents({}) if pixels_col is not None else 0
    return {"online": online_count, "pixels": total_pixels}

@app.get("/leaderboard")
async def leaderboard(period: str = "daily"):
    if users_col is None:
        return []
    if period == "daily":
        cursor = users_col.find({"day_key": day_key(), "pixels_placed_day": {"$gt": 0}}).sort("pixels_placed_day", DESCENDING).limit(10)
        return [{"rank": i+1, "username": u["username"], "display_name": u.get("display_name") or u["username"],
                 "score": u.get("pixels_placed_day", 0), "avatar_color": u.get("avatar_color", 0)}
                for i, u in enumerate(cursor)]
    cursor = users_col.find({"pixels_placed": {"$gt": 0}}).sort("pixels_placed", DESCENDING).limit(20)
    return [{"rank": i+1, "username": u["username"], "display_name": u.get("display_name") or u["username"],
             "score": u.get("pixels_placed", 0), "avatar_color": u.get("avatar_color", 0)}
            for i, u in enumerate(cursor)]

# ---------- REGEN AFK: +1 every 2 seconds = 0.5/s, or higher with rank ----------
@app.post("/mod/lb_zero")
async def mod_lb_zero(data: ModAction, user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = (data.target or "").strip().lstrip("@").lower()
    users_col.update_one({"username": target}, {"$set": {"pixels_placed": 0, "pixels_placed_day": 0}})
    return {"ok": True}

@app.post("/mod/lb_set")
async def mod_lb_set(data: dict = Body(...), user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = str(data.get("target") or "").strip().lstrip("@").lower()
    score = int(data.get("score") or 0)
    period = data.get("period") or "alltime"
    if period == "daily":
        users_col.update_one({"username": target}, {"$set": {"pixels_placed_day": max(0, score), "day_key": day_key()}})
    else:
        users_col.update_one({"username": target}, {"$set": {"pixels_placed": max(0, score)}})
    return {"ok": True}

@app.post("/regen")
async def regen(user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    now = datetime.utcnow()
    last = user.get("last_regen") or now
    if isinstance(last, str):
        try:
            last = datetime.fromisoformat(last.replace("Z", ""))
        except Exception:
            last = now
    elapsed = max(0.0, (now - last).total_seconds())
    rate = rank_bonuses(user)["afk_rate"]  # default 0.5
    rate *= event_afk_multiplier()
    rate += event_afk_add()
    ub = get_user_event_bonus(user.get("username") or "")
    rate = rate * float(ub.get("afk_mul") or 1.0) + float(ub.get("afk_add") or 0)
    if user.get("is_twink"):
        rate = rate / 3
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

# ---------- EARN: animal / math / color ----------
ANIMALS = ["cat","dog","tiger","bear","antelope","elephant","hippo","crocodile","human","mosquito","pig","fox","wolf","rabbit","owl","snake","deer","lion","panda","koala"]
EARN_COLORS = ["#ef4444","#f97316","#eab308","#22c55e","#14b8a6","#3b82f6","#8b5cf6","#ec4899","#78716c"]

@app.get("/earn/start")
async def earn_start(user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    limit = get_pixel_limit(user)
    cur = int(user.get("pixels_left") or 0)
    if cur >= limit:
        raise HTTPException(400, "Limit reached — spend pixels first")
    sid = str(random.randint(100000, 999999))
    roll = random.random()
    if roll < 0.34:
        # animal
        correct = random.choice(ANIMALS)
        options = random.sample([a for a in ANIMALS if a != correct], 8) + [correct]
        random.shuffle(options)
        earn_sessions[sid] = {"type": "animal", "correct": correct, "reward": 15, "chance": 100}
        if len(earn_sessions) > 20000:
            earn_sessions.clear()
        return {"session_id": sid, "mode": "animal", "options": options, "animal": correct, "cooldown": 15, "chance": 100, "reward": 15}
    elif roll < 0.67:
        # math tiers
        r = random.random()
        if r < 0.70:
            a, b = random.randint(1, 30), random.randint(1, 20)
            reward, chance = 15, 70
        elif r < 0.90:
            a, b = random.randint(1, 50), random.randint(1, 40)
            reward, chance = 20, 20
        else:
            a, b = random.randint(1, 130), random.randint(10, 80)
            reward, chance = 25, 10
        op = random.choice(["+", "-"])
        correct = a + b if op == "+" else a - b
        earn_sessions[sid] = {"type": "math", "correct": str(correct), "reward": reward, "chance": chance}
        if len(earn_sessions) > 20000:
            earn_sessions.clear()
        return {"session_id": sid, "mode": "math", "question": f"{a} {op} {b}", "cooldown": 15, "chance": chance, "reward": reward}
    else:
        # colors
        correct = random.choice(EARN_COLORS)
        opts = list(EARN_COLORS)
        random.shuffle(opts)
        earn_sessions[sid] = {"type": "color", "correct": correct, "reward": 15, "chance": 100}
        if len(earn_sessions) > 20000:
            earn_sessions.clear()
        return {"session_id": sid, "mode": "color", "color": correct, "options": opts, "cooldown": 15, "chance": 100, "reward": 15}

@app.post("/earn/check")
async def earn_check(data: EarnCheck, session_id: str = Query(...), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Login required")
    sess = earn_sessions.pop(session_id, None)
    if not sess:
        raise HTTPException(400, "Session expired")
    correct = sess["correct"]
    ans = str(data.answer).strip().lower()
    ok = ans == str(correct).strip().lower()
    if sess.get("type") == "color":
        ok = ans == str(correct).strip().lower()
    if not ok:
        users_col.update_one({"_id": user["_id"]}, {"$set": {"earn_streak": 0}})
        return {"ok": False, "next_cooldown": 15, "correct": correct}
    streak = int(user.get("earn_streak") or 0) + 1
    base = int(sess.get("reward") or 15)
    bonus = rank_bonuses(user)["earn_bonus"]
    add = (base + bonus) * event_earn_multiplier() + event_earn_add()
    ub = get_user_event_bonus(user.get("username") or "")
    add = add * float(ub.get("earn_mul") or 1.0) + float(ub.get("earn_add") or 0)
    if user.get("is_twink"):
        add = add / 3
    add = int(max(0, round(add)))
    limit = get_pixel_limit(user)
    cur = int(user.get("pixels_left") or 0)
    add = min(add, max(0, limit - cur))
    if add <= 0:
        return {"ok": False, "next_cooldown": 15, "msg": "Limit reached"}
    upd = {"$inc": {"pixels_left": add}, "$set": {"earn_streak": streak}}
    if streak >= 5:
        upd["$set"]["streak_medal"] = True
    users_col.update_one({"_id": user["_id"]}, upd)
    return {"ok": True, "added": add, "streak": streak, "next_cooldown": 15, "chance": sess.get("chance"), "reward": base}


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
    # max 1 personal template
    tpls = data.templates[:1]
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
    if not user.get("youtube_medal") and not user.get("is_mod"):
        raise HTTPException(403, "Need YouTube medal to publish")
    # max 1 public template per user
    existing = public_tpl_col.count_documents({"author": user["username"]})
    if existing >= 1 and not user.get("is_mod"):
        raise HTTPException(400, "Max 1 public template — delete old first")
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

# ---------- CHAT + MOD COMMANDS ----------
async def _process_mod_command(raw: str, user: dict) -> Optional[dict]:
    """Handle silent mod slash-commands. Returns response dict or None if not a command."""
    global _global_event, _window_sms
    if not user.get("is_mod"):
        return None
    msg = (raw or "").strip()
    low = msg.lower()
    if not low.startswith("/"):
        return None

    # /all text
    if low.startswith("/all ") or low == "/all":
        text = msg[4:].strip()
        if not text:
            return {"ok": True, "silent": True, "msg": "Usage: /all message"}
        doc_base = {
            "username": user["username"],
            "display_name": user.get("display_name") or user["username"],
            "message": text[:200],
            "is_mod": True,
            "avatar_color": user.get("avatar_color", 0),
            "created_at": datetime.utcnow(),
        }
        for ch in ("en", "ru"):
            d = {**doc_base, "channel": ch}
            if chats_col is not None:
                chats_col.insert_one(d)
                # keep 25
                extra = list(chats_col.find({"channel": ch}).sort("created_at", DESCENDING).skip(25))
                if extra:
                    chats_col.delete_many({"_id": {"$in": [e["_id"] for e in extra]}})
            await manager.broadcast({"type": "chat", **{k: (str(v) if k == "created_at" else v) for k, v in d.items()}})
        # also clan of sender
        if user.get("clan_id"):
            d = {**doc_base, "channel": "clan"}
            if chats_col is not None:
                chats_col.insert_one(d)
            await manager.broadcast({"type": "chat", **{k: (str(v) if k == "created_at" else v) for k, v in d.items()}})
        return {"ok": True, "silent": True, "msg": "Sent to all chats"}

    # /all_clan text
    if low.startswith("/all_clan ") or low.startswith("/all_clan"):
        text = msg.split(" ", 1)[1].strip() if " " in msg else ""
        if not text:
            return {"ok": True, "silent": True, "msg": "Usage: /all_clan message"}
        doc = {
            "username": user["username"],
            "display_name": user.get("display_name") or user["username"],
            "message": text[:200],
            "channel": "clan",
            "is_mod": True,
            "avatar_color": user.get("avatar_color", 0),
            "created_at": datetime.utcnow(),
        }
        if chats_col is not None:
            chats_col.insert_one(doc)
        await manager.broadcast({"type": "chat", "username": doc["username"], "display_name": doc["display_name"],
            "message": doc["message"], "channel": "clan", "is_mod": True,
            "avatar_color": doc["avatar_color"], "created_at": str(doc["created_at"])})
        return {"ok": True, "silent": True, "msg": "Sent to all clans channel"}

    # /abuse_night_55m or /abuse_night_time_true
    m_night = re.match(r"^/abuse_night(?:_time)?(?:_(\d+)m)?(_true)?$", low)
    if m_night or low.startswith("/abuse_night"):
        minutes = 55
        overlay = False
        parts = low.replace("/abuse_night_time", "/abuse_night").replace("/abuse_night", "").strip("_")
        mm = re.search(r"(\d+)m", low)
        if mm:
            minutes = max(1, min(int(mm.group(1)), 24 * 60))
        if low.endswith("_true") or "time_true" in low:
            overlay = True
        _global_event = {
            "mode": "night",
            "ends_at": datetime.utcnow() + timedelta(minutes=minutes),
            "overlay": overlay,
            "set_by": user["username"],
        }
        await manager.broadcast({"type": "event", "mode": "night", "minutes": minutes, "overlay": overlay, "by": user["username"]})
        return {"ok": True, "silent": True, "msg": f"Night event {minutes}m overlay={overlay}"}

    if low in ("/abuse_day", "/abuse_day_time"):
        _global_event = {"mode": None, "ends_at": None, "overlay": False, "set_by": user["username"]}
        await manager.broadcast({"type": "event", "mode": None, "ended": True, "by": user["username"]})
        return {"ok": True, "silent": True, "msg": "Event ended — normal"}

    if low.startswith("/abuse_haos"):
        minutes = 55
        overlay = low.endswith("_true") or "time_true" in low
        mm = re.search(r"(\d+)m", low)
        if mm:
            minutes = max(1, min(int(mm.group(1)), 24 * 60))
        _global_event = {
            "mode": "haos",
            "ends_at": datetime.utcnow() + timedelta(minutes=minutes),
            "overlay": overlay,
            "set_by": user["username"],
        }
        await manager.broadcast({"type": "event", "mode": "haos", "minutes": minutes, "overlay": overlay, "by": user["username"]})
        return {"ok": True, "silent": True, "msg": f"Haos event {minutes}m"}

    if low.startswith("/abuse_won"):
        minutes = 55
        overlay = low.endswith("_true") or "time_true" in low
        mm = re.search(r"(\d+)m", low)
        if mm:
            minutes = max(1, min(int(mm.group(1)), 24 * 60))
        _global_event = {
            "mode": "won",
            "ends_at": datetime.utcnow() + timedelta(minutes=minutes),
            "overlay": overlay,
            "set_by": user["username"],
        }
        await manager.broadcast({"type": "event", "mode": "won", "minutes": minutes, "overlay": overlay, "by": user["username"]})
        return {"ok": True, "silent": True, "msg": f"Won event {minutes}m"}

    # /abuse_blue_300m_80%_EARN(×2)_EARNAFK(×2)_STOCK(+200)
    m_col = re.match(r"^/abuse_(blue|yellow|red|green)_(\d+)m(?:_(\d+)%)?(.*)$", low)
    if m_col:
        color, minutes, opacity_s, rest = m_col.group(1), int(m_col.group(2)), m_col.group(3), m_col.group(4) or ""
        minutes = max(1, min(minutes, 24 * 60))
        opacity = (int(opacity_s) / 100.0) if opacity_s else 0.8
        bonuses = {"earn_mul": 1.0, "earn_add": 0.0, "afk_mul": 1.0, "afk_add": 0.0, "stock_add": 0}
        for part in re.findall(r"[A-Z]+\([^)]+\)", rest.upper().replace(" ", "")):
            bonuses.update(parse_bonus_token(part))
        # also parse without upper for ×
        for part in re.findall(r"(?:EARN|EARNAFK|STOCK)\([^)]+\)", rest, re.I):
            bonuses.update(parse_bonus_token(part.upper().replace("×", "×")))
        # re-parse original tokens
        for part in re.findall(r"(?:EARN|EARNAFK|STOCK)\([^)]+\)", msg, re.I):
            bonuses.update(parse_bonus_token(part))
        _global_event = {
            "mode": color,
            "ends_at": datetime.utcnow() + timedelta(minutes=minutes),
            "overlay": True,
            "set_by": user["username"],
            "opacity": opacity,
            "color": color,
            "earn_mul": bonuses.get("earn_mul", 1.0),
            "earn_add": bonuses.get("earn_add", 0.0),
            "afk_mul": bonuses.get("afk_mul", 1.0),
            "afk_add": bonuses.get("afk_add", 0.0),
            "stock_add": int(bonuses.get("stock_add", 0)),
            "select": None,
        }
        await manager.broadcast({"type": "event", **{k: (str(v) if k == "ends_at" else v) for k, v in _global_event.items()}})
        return {"ok": True, "silent": True, "msg": f"{color} event {minutes}m opacity={opacity}"}

    # /abuse_select_1=EARN(+3)_2=EARNAFK(+0.02)_3=EARN(+4)_EARNAFK(-0.05)_4=STOCK(+20)_TIME(30m)
    if low.startswith("/abuse_select"):
        opts = {}
        for i in range(1, 5):
            opts[str(i)] = {}
        for m in re.finditer(r"(\d)=((?:EARN|EARNAFK|STOCK)\([^)]+\)(?:_(?:EARN|EARNAFK|STOCK)\([^)]+\))*)", msg, re.I):
            num, blob = m.group(1), m.group(2)
            b = {}
            for part in re.findall(r"(?:EARN|EARNAFK|STOCK)\([^)]+\)", blob, re.I):
                b.update(parse_bonus_token(part))
            opts[num] = b
        tm = re.search(r"TIME\((\d+)m\)", msg, re.I)
        duration = int(tm.group(1)) if tm else 30
        duration = max(1, min(duration, 24 * 60))
        _global_event = {
            "mode": "select",
            "ends_at": datetime.utcnow() + timedelta(minutes=duration + 2),  # choice window + bonus
            "overlay": True,
            "set_by": user["username"],
            "opacity": 0.9,
            "color": "blue",
            "earn_mul": 1.0, "earn_add": 0.0, "afk_mul": 1.0, "afk_add": 0.0, "stock_add": 0,
            "select": {
                "options": opts,
                "choice_deadline": (datetime.utcnow() + timedelta(seconds=15)).isoformat(),
                "bonus_start_delay_s": 60,
                "duration_m": duration,
            },
        }
        await manager.broadcast({"type": "event", "mode": "select", "select": _global_event["select"], "by": user["username"], "duration_m": duration})
        # bot SETTINGS announce
        doc = {
            "username": "settings", "display_name": "SETTINGS", "message": f"SELECT event started · bonuses in 1m after choice · lasts {duration}m",
            "channel": "en", "is_mod": True, "avatar_color": 0, "created_at": datetime.utcnow(),
        }
        if chats_col is not None:
            chats_col.insert_one(doc)
        await manager.broadcast({"type": "chat", **{k: (str(v) if k == "created_at" else v) for k, v in doc.items()}})
        return {"ok": True, "silent": True, "msg": f"Select event {duration}m"}

    # /mute_@user_55m
    m_mute = re.match(r"^/mute_@?([a-z0-9_]{2,20})_(\d+)m$", low)
    if m_mute:
        target, minutes = m_mute.group(1), int(m_mute.group(2))
        minutes = max(1, min(minutes, 7 * 24 * 60))
        until = datetime.utcnow() + timedelta(minutes=minutes)
        users_col.update_one({"username": target}, {"$set": {"muted_until": until, "muted_by": user["username"]}})
        return {"ok": True, "silent": True, "msg": f"Muted @{target} {minutes}m"}

    # /window_sms text
    if low.startswith("/window_sms ") or low.startswith("/window_sms"):
        text = msg[len("/window_sms"):].strip()
        if not text:
            return {"ok": True, "silent": True, "msg": "Usage: /window_sms text"}
        _window_sms = {
            "text": text[:400],
            "from": user["username"],
            "display_name": user.get("display_name") or user["username"],
            "created": datetime.utcnow().isoformat(),
        }
        await manager.broadcast({"type": "window_sms", **_window_sms})
        return {"ok": True, "silent": True, "msg": "Window SMS sent"}

    return None


@app.post("/chat")
async def send_chat(message: str = Body(..., embed=True), channel: str = Body("en"), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    muted = user.get("muted_until")
    if muted and muted > datetime.utcnow():
        left = int((muted - datetime.utcnow()).total_seconds() // 60) + 1
        raise HTTPException(403, f"MUTED_BY:@{user.get('muted_by', 'mod')}|MIN:{left}")
    # mod silent commands
    cmd = await _process_mod_command(message, user)
    if cmd is not None:
        return cmd
    # ?time — event info
    if message.strip().lower() in ("?time", "/time"):
        ev = get_global_event()
        if not ev.get("mode"):
            return {"ok": True, "silent": True, "msg": "No active event"}
        ends = ev.get("ends_at")
        left_m = 0
        if isinstance(ends, datetime):
            left_m = max(0, int((ends - datetime.utcnow()).total_seconds() // 60))
        msg_info = f"Event: {ev.get('mode')} · ends in ~{left_m}m · ends_at={ends}"
        return {"ok": True, "silent": True, "msg": msg_info}
    # /take_200 clan pool
    m_take = re.match(r"^/take_(\d+)$", message.strip().lower())
    if m_take and user.get("clan_id"):
        amt = int(m_take.group(1))
        amt = max(1, min(amt, 500))
        cid = str(user["clan_id"])
        pool = int(_clan_pools.get(cid, 0))
        if pool < amt:
            return {"ok": True, "silent": True, "msg": f"Clan pool only {pool}/500"}
        limit = get_pixel_limit(user)
        cur = int(user.get("pixels_left") or 0)
        room = max(0, limit - cur)
        take = min(amt, room)
        if take <= 0:
            return {"ok": True, "silent": True, "msg": "Your stock is full"}
        _clan_pools[cid] = pool - take
        users_col.update_one({"_id": user["_id"]}, {"$inc": {"pixels_left": take}})
        return {"ok": True, "silent": True, "msg": f"Took {take} from clan pool ({_clan_pools[cid]}/500 left)"}
    if len(message) > 200:
        raise HTTPException(400, "Too long")
    if channel not in ("en", "ru", "clan"):
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
        keep = 25
        extra = list(chats_col.find({"channel": channel}).sort("created_at", DESCENDING).skip(keep))
        if extra:
            chats_col.delete_many({"_id": {"$in": [e["_id"] for e in extra]}})
    await manager.broadcast({"type": "chat", "username": doc["username"], "display_name": doc["display_name"],
        "message": doc["message"], "channel": channel, "is_mod": doc["is_mod"],
        "avatar_color": doc["avatar_color"], "created_at": str(doc["created_at"])})
    return {"ok": True}

@app.get("/chat/history")
async def chat_history(channel: str = "en", limit: int = 25):
    if chats_col is None:
        return []
    if channel not in ("en", "ru", "clan"):
        channel = "en"
    lim = 25
    cursor = chats_col.find({"channel": channel}).sort("created_at", DESCENDING).limit(min(limit, lim))
    return [{"username": c["username"], "display_name": c.get("display_name"), "message": c["message"],
             "is_mod": c.get("is_mod", False), "avatar_color": c.get("avatar_color", 0),
             "created_at": str(c["created_at"])} for c in cursor][::-1]


@app.get("/radar")
async def get_radar(user=Depends(get_current_user)):
    """Recent pixels (last 120s / 2 min) — auto-purged from RAM."""
    _prune_recent_pixels()
    if not _recent_pixels:
        return {"recent": [], "last": None}
    # prefer last by someone else
    last = _recent_pixels[-1]
    if user:
        for r in reversed(_recent_pixels):
            if r.get("username") != user.get("username"):
                last = r
                break
    return {
        "recent": [{"x": r["x"], "y": r["y"], "color": r["color"], "username": r["username"], "t": r["t"]} for r in _recent_pixels[-30:]],
        "last": {"x": last["x"], "y": last["y"], "color": last["color"], "username": last["username"], "t": last["t"]},
        # compat fields
        "x": last["x"], "y": last["y"], "color": last["color"], "username": last["username"], "t": last["t"],
    }

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

@app.get("/event")
async def get_event():
    ev = get_global_event()
    out = {
        "mode": ev.get("mode"),
        "overlay": bool(ev.get("overlay")),
        "set_by": ev.get("set_by"),
        "ends_at": str(ev["ends_at"]) if ev.get("ends_at") else None,
        "opacity": ev.get("opacity", 0.8),
        "color": ev.get("color") or ev.get("mode"),
        "earn_mul": ev.get("earn_mul", 1),
        "earn_add": ev.get("earn_add", 0),
        "afk_mul": ev.get("afk_mul", 1),
        "afk_add": ev.get("afk_add", 0),
        "stock_add": ev.get("stock_add", 0),
        "select": ev.get("select"),
    }
    return out

@app.get("/window_sms")
async def get_window_sms():
    return _window_sms or {}

@app.post("/news/delete")
async def news_delete(nid: str = Body(..., embed=True), user=Depends(get_current_user)):
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    if news_col is None:
        raise HTTPException(503)
    try:
        news_col.delete_one({"_id": ObjectId(nid)})
    except Exception:
        raise HTTPException(400, "Bad id")
    return {"ok": True}

@app.post("/accounts/create_twink")
async def create_twink(slot: int = Body(1, embed=True), user=Depends(get_current_user)):
    """Create a twink sub-account (slot 1 or 2). Debuff x3 on limits/earn/afk."""
    if not user:
        raise HTTPException(401)
    if user.get("is_twink"):
        raise HTTPException(400, "Twinks cannot create twinks")
    slot = 1 if slot not in (1, 2) else slot
    uname = f"{user['username']}_twink_{slot}"
    if users_col.find_one({"username": uname}):
        raise HTTPException(400, "Twink already exists")
    # max 2 twinks for parent
    existing = list(users_col.find({"parent_username": user["username"], "is_twink": True}))
    if len(existing) >= 2:
        raise HTTPException(400, "Max 2 twinks")
    doc = {
        "username": uname,
        "display_name": f"{user.get('display_name') or user['username']} T{slot}",
        "password": user.get("password"),  # same hash — login with twink name + same password
        "pixels_left": 20,
        "pixels_placed": 0,
        "pixels_placed_day": 0,
        "day_key": day_key(),
        "is_mod": False,
        "is_twink": True,
        "twink_slot": slot,
        "parent_username": user["username"],
        "created_at": datetime.utcnow(),
    }
    users_col.insert_one(doc)
    token = create_access_token({"sub": uname})
    fresh = users_col.find_one({"username": uname})
    return {"ok": True, "access_token": token, "user": {**user_public(fresh), "limit": get_pixel_limit(fresh)}}

@app.post("/accounts/link")
async def link_account(username: str = Body(...), password: str = Body(...), user=Depends(get_current_user)):
    """Link an existing normal account into swipe list (no twink debuff). Stored client-side mainly; validates credentials."""
    if not user:
        raise HTTPException(401)
    uname = username.strip().lstrip("@").lower()
    target = users_col.find_one({"username": uname})
    if not target or not verify_password(password, target["password"]):
        raise HTTPException(401, "Wrong username or password")
    token = create_access_token({"sub": target["username"]})
    return {"ok": True, "access_token": token, "user": {**user_public(target), "limit": get_pixel_limit(target)}, "is_twink": bool(target.get("is_twink"))}

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


# ---------- VIDEOS / QUESTIONS / REPORTS / SELECT / CLAN BOOST ----------
videos_col = None
questions_col = None
reports_col = None

def _ensure_extra_cols():
    global videos_col, questions_col, reports_col
    if db is None:
        return
    if videos_col is None:
        videos_col = db["videos"]
    if questions_col is None:
        questions_col = db["questions"]
    if reports_col is None:
        reports_col = db["reports"]

@app.get("/videos")
async def list_videos():
    _ensure_extra_cols()
    if videos_col is None:
        return []
    out = []
    for v in videos_col.find().sort("created", -1).limit(50):
        out.append({
            "id": str(v["_id"]), "title": v.get("title"), "author": v.get("author"),
            "author_name": v.get("author_name"), "about": v.get("about"), "url": v.get("url"),
            "created": str(v.get("created")), "updated": str(v.get("updated") or ""),
        })
    return out

@app.post("/videos")
async def post_video(data: dict = Body(...), user=Depends(get_current_user)):
    _ensure_extra_cols()
    if not user:
        raise HTTPException(401)
    if videos_col is None:
        raise HTTPException(503)
    # 5 hour cooldown between new videos
    last = videos_col.find_one({"author": user["username"]}, sort=[("created", -1)])
    if last and last.get("created"):
        delta = datetime.utcnow() - last["created"]
        if delta.total_seconds() < 5 * 3600 and not user.get("is_mod"):
            left = int((5 * 3600 - delta.total_seconds()) // 60)
            raise HTTPException(400, f"Wait {left}m before new video")
    title = str(data.get("title") or "")[:80]
    about = str(data.get("about") or "")[:400]
    url = str(data.get("url") or "")[:500]
    if not title or not url:
        raise HTTPException(400, "Title and URL required")
    videos_col.insert_one({
        "title": title, "about": about, "url": url,
        "author": user["username"], "author_name": user.get("display_name") or user["username"],
        "created": datetime.utcnow(), "updated": datetime.utcnow(),
    })
    return {"ok": True}

@app.post("/videos/edit")
async def edit_video(data: dict = Body(...), user=Depends(get_current_user)):
    _ensure_extra_cols()
    if not user:
        raise HTTPException(401)
    try:
        oid = ObjectId(str(data.get("id")))
    except Exception:
        raise HTTPException(400, "Bad id")
    v = videos_col.find_one({"_id": oid})
    if not v or (v.get("author") != user["username"] and not user.get("is_mod")):
        raise HTTPException(403)
    # 10 min edit cooldown
    upd_at = v.get("updated") or v.get("created")
    if upd_at and not user.get("is_mod"):
        if (datetime.utcnow() - upd_at).total_seconds() < 600:
            raise HTTPException(400, "Edit once per 10 minutes")
    fields = {}
    for k in ("title", "about", "url"):
        if k in data and data[k] is not None:
            fields[k] = str(data[k])[:500]
    fields["updated"] = datetime.utcnow()
    videos_col.update_one({"_id": oid}, {"$set": fields})
    return {"ok": True}

@app.get("/questions")
async def list_questions(user=Depends(get_current_user)):
    _ensure_extra_cols()
    if questions_col is None:
        return []
    q = {"published": True}
    if user and user.get("is_mod"):
        q = {}
    out = []
    for item in questions_col.find(q).sort("created", -1).limit(50):
        out.append({
            "id": str(item["_id"]), "text": item.get("text"), "author": item.get("author"),
            "answer": item.get("answer"), "published": bool(item.get("published")),
            "created": str(item.get("created")),
        })
    return out

@app.post("/questions")
async def post_question(data: dict = Body(...), user=Depends(get_current_user)):
    _ensure_extra_cols()
    if not user:
        raise HTTPException(401)
    text = str(data.get("text") or "").strip()[:500]
    if not text:
        raise HTTPException(400)
    questions_col.insert_one({
        "text": text, "author": user["username"], "answer": None, "published": False,
        "created": datetime.utcnow(),
    })
    return {"ok": True}

@app.post("/questions/answer")
async def answer_question(data: dict = Body(...), user=Depends(get_current_user)):
    _ensure_extra_cols()
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    try:
        oid = ObjectId(str(data.get("id")))
    except Exception:
        raise HTTPException(400)
    answer = str(data.get("answer") or "")[:1000]
    publish = bool(data.get("publish"))
    questions_col.update_one({"_id": oid}, {"$set": {"answer": answer, "published": publish}})
    return {"ok": True}

@app.post("/report")
async def post_report(data: dict = Body(...), user=Depends(get_current_user)):
    _ensure_extra_cols()
    if not user:
        raise HTTPException(401)
    target = str(data.get("target") or "").strip().lstrip("@").lower()[:30]
    reason = str(data.get("reason") or "")[:80]
    comment = str(data.get("comment") or "")[:400]
    if not target:
        raise HTTPException(400, "Target required")
    reports_col.insert_one({
        "target": target, "reason": reason, "comment": comment,
        "by": user["username"], "created": datetime.utcnow(), "status": "open",
    })
    return {"ok": True}

@app.get("/reports")
async def list_reports(user=Depends(get_current_user)):
    _ensure_extra_cols()
    if not user or not user.get("is_mod"):
        raise HTTPException(403)
    out = []
    for r in reports_col.find().sort("created", -1).limit(100):
        out.append({
            "id": str(r["_id"]), "target": r.get("target"), "reason": r.get("reason"),
            "comment": r.get("comment"), "by": r.get("by"), "created": str(r.get("created")),
            "status": r.get("status"),
        })
    return out

@app.post("/event/select")
async def event_select_choice(data: dict = Body(...), user=Depends(get_current_user)):
    """Player picks option 1-4 during select event."""
    if not user:
        raise HTTPException(401)
    ev = get_global_event()
    if ev.get("mode") != "select" or not ev.get("select"):
        raise HTTPException(400, "No select event")
    choice = str(data.get("choice") or "")
    opts = (ev.get("select") or {}).get("options") or {}
    if choice not in opts:
        raise HTTPException(400, "Bad choice")
    # schedule bonus after 60s for duration_m
    sel = ev["select"]
    duration_m = int(sel.get("duration_m") or 30)
    delay = int(sel.get("bonus_start_delay_s") or 60)
    bonus = dict(opts[choice])
    bonus["ends_at"] = datetime.utcnow() + timedelta(seconds=delay + duration_m * 60)
    bonus["starts_at"] = datetime.utcnow() + timedelta(seconds=delay)
    _user_event_bonus[user["username"]] = bonus
    return {"ok": True, "choice": choice, "bonus": {k: (str(v) if isinstance(v, datetime) else v) for k, v in bonus.items()}}

@app.post("/clan/boost")
async def clan_boost(user=Depends(get_current_user)):
    """Overlord boost: +50 to all online clan members. CD 5h. Requires 20 members."""
    if not user or not user.get("clan_id"):
        raise HTTPException(400, "No clan")
    clan = clans_col.find_one({"_id": ObjectId(user["clan_id"])})
    if not clan:
        raise HTTPException(404)
    if clan.get("leader") != user["username"]:
        raise HTTPException(403, "Leader only")
    members = clan.get("members") or []
    if len(members) < 20:
        raise HTTPException(400, "Need 20 members for Overlord boost")
    last = clan.get("boost_at")
    if last and (datetime.utcnow() - last).total_seconds() < 5 * 3600:
        left = int((5 * 3600 - (datetime.utcnow() - last).total_seconds()) // 60)
        raise HTTPException(400, f"Cooldown {left}m")
    online_names = set()
    for info in online_users.values():
        un = info.get("username")
        if un and un in members:
            online_names.add(un)
    given = 0
    overflow = 0
    for un in online_names:
        u = users_col.find_one({"username": un})
        if not u:
            continue
        limit = get_pixel_limit(u)
        cur = int(u.get("pixels_left") or 0)
        room = max(0, limit - cur)
        add = min(50, room)
        overflow += 50 - add
        if add > 0:
            users_col.update_one({"_id": u["_id"]}, {"$inc": {"pixels_left": add}})
            given += 1
    cid = str(clan["_id"])
    _clan_pools[cid] = min(500, int(_clan_pools.get(cid, 0)) + overflow)
    clans_col.update_one({"_id": clan["_id"]}, {"$set": {"boost_at": datetime.utcnow()}})
    await manager.broadcast({
        "type": "clan_boost",
        "from": user["username"],
        "display_name": user.get("display_name") or user["username"],
        "clan_id": cid,
        "amount": 50,
        "pool": _clan_pools[cid],
    })
    return {"ok": True, "online": given, "overflow": overflow, "pool": _clan_pools[cid]}

@app.get("/clan/pool")
async def clan_pool(user=Depends(get_current_user)):
    if not user or not user.get("clan_id"):
        return {"pool": 0, "max": 500}
    return {"pool": int(_clan_pools.get(str(user["clan_id"]), 0)), "max": 500}

@app.get("/event_full")
async def get_event_full():
    ev = get_global_event()
    out = {
        "mode": ev.get("mode"),
        "overlay": bool(ev.get("overlay")),
        "set_by": ev.get("set_by"),
        "ends_at": str(ev["ends_at"]) if ev.get("ends_at") else None,
        "opacity": ev.get("opacity", 0.8),
        "color": ev.get("color") or ev.get("mode"),
        "earn_mul": ev.get("earn_mul", 1),
        "earn_add": ev.get("earn_add", 0),
        "afk_mul": ev.get("afk_mul", 1),
        "afk_add": ev.get("afk_add", 0),
        "stock_add": ev.get("stock_add", 0),
        "select": ev.get("select"),
    }
    return out


# ---------- WS / STATIC ----------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            try:
                # 60s without any message from client → treat as dead
                data = await asyncio.wait_for(ws.receive_text(), timeout=60.0)
            except asyncio.TimeoutError:
                try:
                    await ws.close(code=1000)
                except Exception:
                    pass
                break
            if data == "ping":
                try:
                    await ws.send_text("pong")
                except Exception:
                    break
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
        pass
    except Exception:
        pass
    finally:
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


@app.post("/mod/wipe_user")
async def mod_wipe_user(data: ModAction, user=Depends(get_current_user)):
    """Delete ALL info about target: account, pixels, templates, records."""
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    target = (data.target or "").strip().lstrip("@").lower()
    if not target:
        raise HTTPException(400, "Target required")
    if target == user.get("username"):
        raise HTTPException(400, "Cannot wipe yourself")
    t = users_col.find_one({"username": target}) if users_col is not None else None
    if not t:
        raise HTTPException(404, "User not found")
    deleted_px = 0
    if pixels_col is not None:
        res = pixels_col.delete_many({"username": target})
        deleted_px = res.deleted_count
        # also by user_id
        try:
            res2 = pixels_col.delete_many({"user_id": str(t["_id"])})
            deleted_px += res2.deleted_count
        except Exception:
            pass
        # refresh cache roughly
        global _pixel_cache_ready
        _pixel_cache_ready = False
    if public_tpl_col is not None:
        public_tpl_col.delete_many({"author": target})
    if chats_col is not None:
        chats_col.delete_many({"username": target})
    # remove from clans
    if clans_col is not None:
        for clan in clans_col.find({"$or": [{"leader": target}, {"members": target}]}):
            members = [m for m in (clan.get("members") or []) if m != target]
            if clan.get("leader") == target:
                clans_col.delete_one({"_id": clan["_id"]})
                for m in members:
                    users_col.update_one({"username": m}, {"$unset": {"clan_id": "", "clan_tag": ""}})
            else:
                clans_col.update_one({"_id": clan["_id"]}, {"$pull": {"members": target}})
    users_col.delete_one({"_id": t["_id"]})
    await manager.broadcast({"type": "wipe", "username": target, "deleted_pixels": deleted_px})
    return {"ok": True, "deleted_pixels": deleted_px, "username": target}

@app.post("/mod/templates_clear")
async def mod_templates_clear(user=Depends(get_current_user)):
    """Delete ALL public templates."""
    if not user or not user.get("is_mod"):
        raise HTTPException(403, "Mod only")
    if public_tpl_col is None:
        return {"ok": True, "deleted": 0}
    res = public_tpl_col.delete_many({})
    return {"ok": True, "deleted": res.deleted_count}

@app.post("/templates/delete_public")
async def delete_public_template(tid: str = Body(..., embed=True), user=Depends(get_current_user)):
    if not user:
        raise HTTPException(401)
    if public_tpl_col is None:
        raise HTTPException(503)
    try:
        oid = ObjectId(tid)
    except Exception:
        raise HTTPException(400, "Bad id")
    t = public_tpl_col.find_one({"_id": oid})
    if not t:
        raise HTTPException(404)
    if t.get("author") != user["username"] and not user.get("is_mod"):
        raise HTTPException(403, "Not yours")
    public_tpl_col.delete_one({"_id": oid})
    return {"ok": True}

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
    px_count = None
    try:
        if pixels_col is not None:
            px_count = pixels_col.estimated_document_count()
    except Exception as e:
        px_count = str(e)
    return {
        "status": "ok",
        "mongo": users_col is not None,
        "mongo_uri_set": bool(MONGO_URI),
        "pixels_in_db": px_count,
        "index_html": _find_file("index.html"),
        "world_map": _find_file("world_map.png"),
        "listings": listings,
        "online": online_count,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
