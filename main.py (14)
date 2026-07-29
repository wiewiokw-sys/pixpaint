"""PixelGame Python 3.14 - no pydantic/rust"""
import os, time, random, secrets, hashlib, asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Set, Optional

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, FileResponse, HTMLResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
import jwt
from pymongo import MongoClient
from bson import ObjectId

MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://user:pass@cluster.mongodb.net/pixelgame?retryWrites=true&w=majority")
SECRET_KEY = os.environ.get("SECRET_KEY", "change-this-secret-key-in-production-2026")
ALGORITHM = "HS256"
TOKEN_EXPIRE_MIN = 60 * 24 * 7
CANVAS_W, CANVAS_H, CHUNK = 4096, 4096, 256
GUEST_MAX, AUTH_MAX = 60, 200
DISCORD_BONUS, MOD_BONUS, CLAN_MEMBER_BONUS, CLAN_MAX = 100, 15, 5, 20
EARN_REWARD, EARN_OK_CD, EARN_FAIL_CD = 15, 7.0, 5.0
MOD_CODE = "237360049320122092250232257"

PALETTE = ["#000000","#FFFFFF","#C0C0C0","#808080","#404040","#FF0000","#FF4000","#FF8000","#FFBF00","#FFFF00","#BFFF00","#80FF00","#40FF00","#00FF00","#00FF40","#00FF80","#00FFBF","#00FFFF","#00BFFF","#0080FF","#0040FF","#0000FF","#4000FF","#8000FF","#BF00FF","#FF00FF","#FF00BF","#FF0080","#FF0040","#800000","#804000","#808000","#408000","#008000","#008040","#008080","#004080","#000080","#400080","#800080","#FF6666","#FFB366","#FFFF66","#B3FF66","#66FFB3","#66B3FF","#B366FF","#FF66B3","#A0522D","#D2691E"]

ANIMALS = [{"id":"cat","name":"Cat","emoji":"🐱"},{"id":"dog","name":"Dog","emoji":"🐶"},{"id":"tiger","name":"Tiger","emoji":"🐯"},{"id":"bear","name":"Bear","emoji":"🐻"},{"id":"antelope","name":"Antelope","emoji":"🦌"},{"id":"elephant","name":"Elephant","emoji":"🐘"},{"id":"hippo","name":"Hippo","emoji":"🦛"},{"id":"crocodile","name":"Crocodile","emoji":"🐊"},{"id":"human","name":"Human","emoji":"🧑"},{"id":"mosquito","name":"Mosquito","emoji":"🦟"},{"id":"pig","name":"Pig","emoji":"🐷"},{"id":"fox","name":"Fox","emoji":"🦊"},{"id":"wolf","name":"Wolf","emoji":"🐺"},{"id":"rabbit","name":"Rabbit","emoji":"🐰"},{"id":"owl","name":"Owl","emoji":"🦉"},{"id":"frog","name":"Frog","emoji":"🐸"},{"id":"snake","name":"Snake","emoji":"🐍"},{"id":"lion","name":"Lion","emoji":"🦁"},{"id":"panda","name":"Panda","emoji":"🐼"},{"id":"koala","name":"Koala","emoji":"🐨"},{"id":"monkey","name":"Monkey","emoji":"🐵"}]
SVG_DEFAULT = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" width="120" height="120"><circle cx="32" cy="32" r="20" fill="#88CC88"/><text x="32" y="38" text-anchor="middle" font-size="28">?</text></svg>'

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
db = client["pixelgame"]
try:
    db.users.create_index("username", unique=True)
    db.pixels.create_index([("x", 1), ("y", 1)], unique=True)
    db.clans.create_index("name", unique=True)
except Exception as e:
    print("[DB]", e)

canvas_chunks: Dict[str, Dict[str, int]] = {}
ws_clients: Set[WebSocket] = set()
guest_challenges: Dict = {}
BASE = Path(__file__).resolve().parent

def hash_pw(p): return hashlib.sha256((p + SECRET_KEY).encode()).hexdigest()
def verify_pw(p, h): return hash_pw(p) == h
def make_token(u):
    return jwt.encode({"sub": u, "exp": datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_MIN)}, SECRET_KEY, algorithm=ALGORITHM)
def decode_token(t):
    if not t: return None
    try: return jwt.decode(t, SECRET_KEY, algorithms=[ALGORITHM]).get("sub")
    except Exception: return None
def ck(x,y): return f"{x//CHUNK}_{y//CHUNK}"
def pk(x,y): return f"{x},{y}"
def calc_max(user):
    base = AUTH_MAX if user.get("password_hash") else GUEST_MAX
    bonus = 0
    if user.get("is_mod"): bonus += MOD_BONUS
    if user.get("has_discord"): bonus += DISCORD_BONUS
    if user.get("clan_id"):
        clan = db.clans.find_one({"_id": user["clan_id"]})
        if clan: bonus += min(len(clan.get("members",[])), CLAN_MAX) * CLAN_MEMBER_BONUS
    return base + bonus
async def broadcast(data):
    dead = []
    for ws in list(ws_clients):
        try: await ws.send_json(data)
        except Exception: dead.append(ws)
    for d in dead: ws_clients.discard(d)
def err(msg, code=400): return JSONResponse({"detail": msg}, status_code=code)

async def register(request: Request):
    d = await request.json()
    username = str(d.get("username","")).strip()
    password = str(d.get("password",""))
    if len(username)<3 or len(username)>24: return err("Username 3-24 chars")
    if len(password)<4: return err("Password too short")
    if db.users.find_one({"username": username.lower()}): return err("Username taken")
    db.users.insert_one({"username":username.lower(),"display_name":username,"password_hash":hash_pw(password),"pixels":0,"is_mod":False,"has_discord":False,"clan_id":None,"total_placed":0,"language":"en","last_earn":0.0,"muted_until":0.0,"place_ban_until":0.0,"banned":False,"created_at":datetime.utcnow()})
    return JSONResponse({"token":make_token(username.lower()),"username":username})

async def login(request: Request):
    d = await request.json()
    u = db.users.find_one({"username": str(d.get("username","")).lower()})
    if not u or not verify_pw(str(d.get("password","")), u["password_hash"]): return err("Invalid credentials",401)
    if u.get("banned"): return err("Banned",403)
    return JSONResponse({"token":make_token(u["username"]),"username":u.get("display_name",u["username"]),"pixels":u.get("pixels",0),"max_pixels":calc_max(u),"is_mod":u.get("is_mod",False),"has_discord":u.get("has_discord",False),"language":u.get("language","en")})

async def me(request: Request):
    auth = request.headers.get("authorization","")
    if not auth.startswith("Bearer "): return err("No token",401)
    un = decode_token(auth[7:])
    if not un: return err("Invalid token",401)
    u = db.users.find_one({"username": un})
    if not u: return err("Not found",404)
    medals = []
    if u.get("has_discord"): medals.append("discord")
    if u.get("is_mod"): medals.append("mod")
    if u.get("clan_id"): medals.append("clan")
    return JSONResponse({"username":u.get("display_name",u["username"]),"pixels":u.get("pixels",0),"max_pixels":calc_max(u),"is_mod":u.get("is_mod",False),"has_discord":u.get("has_discord",False),"clan_id":str(u["clan_id"]) if u.get("clan_id") else None,"medals":medals,"total_placed":u.get("total_placed",0),"language":u.get("language","en")})

async def get_palette(request: Request):
    return JSONResponse({"palette": PALETTE})

async def get_chunk(request: Request):
    cx, cy = int(request.path_params["cx"]), int(request.path_params["cy"])
    key = f"{cx}_{cy}"
    if key in canvas_chunks:
        pixels = [[int(k.split(",")[0]), int(k.split(",")[1]), v] for k,v in canvas_chunks[key].items()]
        return JSONResponse({"chunk":key,"pixels":pixels})
    x0, y0 = cx*CHUNK, cy*CHUNK
    pixels, chunk_data = [], {}
    for p in db.pixels.find({"x":{"$gte":x0,"$lt":x0+CHUNK},"y":{"$gte":y0,"$lt":y0+CHUNK}}):
        pixels.append([p["x"],p["y"],p["c"]])
        chunk_data[pk(p["x"],p["y"])] = p["c"]
    canvas_chunks[key] = chunk_data
    return JSONResponse({"chunk":key,"pixels":pixels})

async def place(request: Request):
    d = await request.json()
    x, y = int(d["x"]), int(d["y"])
    color = int(d.get("color", d.get("c", 0)))
    token = d.get("token")
    if not (0<=x<CANVAS_W and 0<=y<CANVAS_H): return err("Out of bounds")
    if not (1<=color<len(PALETTE)): return err("Invalid color")
    user = username = None
    if token:
        username = decode_token(token)
        if username:
            user = db.users.find_one({"username": username})
            if user:
                if user.get("banned"):
                    db.users.update_one({"username":username},{"$inc":{"pixels":-30}})
                    return err("Banned. Penalty -30",403)
                if user.get("place_ban_until",0) > time.time():
                    return err("Place restricted %ds" % int(user["place_ban_until"]-time.time()),403)
    new_pixels = None
    if user:
        if user.get("pixels",0)<=0: return err("No pixels left")
        db.users.update_one({"username":username},{"$inc":{"pixels":-1,"total_placed":1}})
        new_pixels = user.get("pixels",0)-1
    key, pkey = ck(x,y), pk(x,y)
    if key not in canvas_chunks: canvas_chunks[key] = {}
    canvas_chunks[key][pkey] = color
    db.pixels.update_one({"x":x,"y":y},{"$set":{"x":x,"y":y,"c":color,"u":username,"t":time.time()}}, upsert=True)
    await broadcast({"type":"pixel","x":x,"y":y,"c":color,"u":username})
    return JSONResponse({"ok":True,"pixels_left":new_pixels})

async def lookup(request: Request):
    x, y = int(request.path_params["x"]), int(request.path_params["y"])
    pix = db.pixels.find_one({"x":x,"y":y})
    if not pix: return JSONResponse({"x":x,"y":y,"empty":True})
    owner = None
    if pix.get("u"):
        u = db.users.find_one({"username": pix["u"]})
        if u:
            medals = []
            clan_name = None
            if u.get("has_discord"): medals.append("discord")
            if u.get("is_mod"): medals.append("mod")
            if u.get("clan_id"):
                medals.append("clan")
                clan = db.clans.find_one({"_id": u["clan_id"]})
                clan_name = clan["name"] if clan else None
            owner = {"username":u.get("display_name",u["username"]),"username_id":u["username"],"is_mod":u.get("is_mod",False),"medals":medals,"clan_name":clan_name,"total_placed":u.get("total_placed",0),"banned":u.get("banned",False)}
    return JSONResponse({"x":x,"y":y,"color":pix.get("c"),"time":pix.get("t"),"owner":owner,"empty":False})

async def mod_action(request: Request):
    d = await request.json()
    mod_name = decode_token(d.get("token"))
    if not mod_name: return err("Invalid token",401)
    mod = db.users.find_one({"username": mod_name})
    if not mod or not mod.get("is_mod"): return err("Not a mod",403)
    t = str(d.get("target","")).lower()
    target = db.users.find_one({"username": t})
    if not target: return err("User not found",404)
    if target.get("is_mod") and target["username"] != mod_name: return err("Cannot moderate another mod",403)
    action = d.get("action")
    now = time.time()
    mins = max(1, min(int(d.get("minutes") or 10), 2880))
    if action == "ban":
        db.users.update_one({"username":t},{"$set":{"banned":True}}); return JSONResponse({"ok":True,"message":f"{t} banned"})
    if action == "unban":
        db.users.update_one({"username":t},{"$set":{"banned":False}}); return JSONResponse({"ok":True,"message":f"{t} unbanned"})
    if action == "delete":
        if target.get("clan_id"): db.clans.update_one({"_id":target["clan_id"]},{"$pull":{"members":t}})
        db.users.delete_one({"username":t}); return JSONResponse({"ok":True,"message":f"Account {t} deleted"})
    if action == "mute":
        db.users.update_one({"username":t},{"$set":{"muted_until":now+mins*60}}); return JSONResponse({"ok":True,"message":f"Muted {mins} min"})
    if action == "place_ban":
        db.users.update_one({"username":t},{"$set":{"place_ban_until":now+mins*60}}); return JSONResponse({"ok":True,"message":f"Place banned {mins} min"})
    return err("Unknown action")

async def activate_mod(request: Request):
    d = await request.json()
    un = decode_token(d.get("token"))
    if not un: return err("Invalid token",401)
    if str(d.get("code","")).strip() != MOD_CODE: return err("Wrong code")
    db.users.update_one({"username":un},{"$set":{"is_mod":True}})
    return JSONResponse({"ok":True,"message":"You are now a moderator"})

async def list_clans(request: Request):
    clans = []
    for c in db.clans.find().sort("name",1):
        clans.append({"id":str(c["_id"]),"name":c["name"],"leader":c.get("leader"),"members":c.get("members",[]),"member_count":len(c.get("members",[])),"max_members":CLAN_MAX})
    return JSONResponse({"clans":clans})

async def create_clan(request: Request):
    d = await request.json()
    un = decode_token(d.get("token"))
    if not un: return err("Login required",401)
    name = str(d.get("name","")).strip()[:24]
    if len(name)<2: return err("Name too short")
    u = db.users.find_one({"username":un})
    if not u: return err("User not found",404)
    if u.get("clan_id"): return err("Already in a clan")
    if db.clans.find_one({"name":{"$regex":f"^{name}$","$options":"i"}}): return err("Name taken")
    res = db.clans.insert_one({"name":name,"leader":un,"members":[un],"created_at":datetime.utcnow()})
    db.users.update_one({"username":un},{"$set":{"clan_id":res.inserted_id}})
    return JSONResponse({"ok":True,"clan_id":str(res.inserted_id),"name":name})

async def join_clan(request: Request):
    d = await request.json()
    un = decode_token(d.get("token"))
    if not un: return err("Login required",401)
    u = db.users.find_one({"username":un})
    if not u: return err("User not found",404)
    if u.get("clan_id"): return err("Already in a clan")
    try: oid = ObjectId(d.get("clan_id"))
    except Exception: return err("Invalid id")
    clan = db.clans.find_one({"_id":oid})
    if not clan: return err("Clan not found",404)
    if len(clan.get("members",[]))>=CLAN_MAX: return err("Full")
    db.clans.update_one({"_id":oid},{"$addToSet":{"members":un}})
    db.users.update_one({"username":un},{"$set":{"clan_id":oid}})
    return JSONResponse({"ok":True})

async def leave_clan(request: Request):
    d = await request.json()
    un = decode_token(d.get("token"))
    if not un: return err("Login required",401)
    u = db.users.find_one({"username":un})
    if not u or not u.get("clan_id"): return err("Not in a clan")
    cid = u["clan_id"]
    clan = db.clans.find_one({"_id":cid})
    if clan:
        db.clans.update_one({"_id":cid},{"$pull":{"members":un}})
        members = [m for m in clan.get("members",[]) if m!=un]
        if clan.get("leader")==un:
            if members: db.clans.update_one({"_id":cid},{"$set":{"leader":members[0]}})
            else: db.clans.delete_one({"_id":cid})
    db.users.update_one({"username":un},{"$set":{"clan_id":None}})
    return JSONResponse({"ok":True})

async def earn_start(request: Request):
    try: d = await request.json()
    except Exception: d = {}
    token = d.get("token") if isinstance(d,dict) else None
    user = username = None
    if token:
        username = decode_token(token)
        if username: user = db.users.find_one({"username":username})
    correct = random.choice(ANIMALS)
    options = [correct["name"]] + [a["name"] for a in random.sample([a for a in ANIMALS if a["id"]!=correct["id"]], 8)]
    random.shuffle(options)
    cid = secrets.token_hex(8)
    now = time.time()
    if user:
        db.users.update_one({"username":username},{"$set":{"earn_challenge":{"id":cid,"animal":correct["id"],"ts":now}}})
    else:
        guest_challenges[cid] = {"animal":correct["id"],"ts":now}
    return JSONResponse({"challenge_id":cid,"animal_id":correct["id"],"svg":SVG_DEFAULT,"emoji":correct["emoji"],"options":options})

async def earn_submit(request: Request):
    d = await request.json()
    user = username = None
    token = d.get("token")
    if token:
        username = decode_token(token)
        if username: user = db.users.find_one({"username":username})
    now = time.time()
    correct_id = None
    if user and user.get("earn_challenge"):
        ch = user["earn_challenge"]
        if ch["id"] != d.get("challenge_id"): return err("Invalid challenge")
        if now - ch["ts"] > 60: return err("Expired")
        correct_id = ch["animal"]
        db.users.update_one({"username":username},{"$unset":{"earn_challenge":""}})
    else:
        ch = guest_challenges.get(d.get("challenge_id"))
        if not ch: return err("Invalid challenge")
        if now - ch["ts"] > 60: return err("Expired")
        correct_id = ch["animal"]
        del guest_challenges[d.get("challenge_id")]
    animal = next(a for a in ANIMALS if a["id"]==correct_id)
    ok = str(d.get("answer","")).lower() == animal["name"].lower()
    reward, cd = 0, EARN_FAIL_CD
    if ok:
        cd = EARN_OK_CD
        reward = EARN_REWARD
        if user:
            mx = calc_max(user)
            add = min(reward, mx - user.get("pixels",0))
            if add > 0:
                db.users.update_one({"username":username},{"$inc":{"pixels":add},"$set":{"last_earn":now}})
                reward = add
            else: reward = 0
    return JSONResponse({"correct":ok,"reward":reward if ok else 0,"cooldown":cd,"correct_name":animal["name"]})

async def send_chat(request: Request):
    d = await request.json()
    channel = d.get("channel","global")
    if channel not in ("global","ru","en","ar","tr"): return err("Invalid channel")
    text = str(d.get("text","")).strip()
    if not text or len(text)>200: return err("Invalid message")
    username = "Guest"
    token = d.get("token")
    if token:
        un = decode_token(token)
        if un:
            u = db.users.find_one({"username":un})
            if u:
                if u.get("banned"): return err("Banned",403)
                if u.get("muted_until",0) > time.time(): return err("Muted",403)
                username = u.get("display_name",un)
    doc = {"channel":channel,"user":username,"text":text,"ts":time.time()}
    db.chat.insert_one(doc)
    await broadcast({"type":"chat",**doc})
    return JSONResponse({"ok":True})

async def get_chat(request: Request):
    channel = request.path_params["channel"]
    msgs = []
    for m in db.chat.find({"channel":channel}).sort("ts",-1).limit(50):
        msgs.append({"user":m["user"],"text":m["text"],"ts":m["ts"]})
    msgs.reverse()
    return JSONResponse({"messages":msgs})

async def online(request: Request):
    return JSONResponse({"count": len(ws_clients)})

async def info(request: Request):
    return JSONResponse({"width":CANVAS_W,"height":CANVAS_H,"palette_size":len(PALETTE),"chunk_size":CHUNK})

async def index(request: Request):
    p = BASE / "index.html"
    if p.exists(): return FileResponse(p)
    return HTMLResponse("<h1>Put index.html next to main.py</h1>")

async def world_map(request: Request):
    p = BASE / "world_map.png"
    if p.exists(): return FileResponse(p, media_type="image/png")
    return err("Map not found",404)

async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.add(websocket)
    await broadcast({"type":"online","count":len(ws_clients)})
    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type")=="ping":
                await websocket.send_json({"type":"pong"})
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(websocket)
        await broadcast({"type":"online","count":len(ws_clients)})

routes = [
    Route("/", index),
    Route("/world_map.png", world_map),
    Route("/api/register", register, methods=["POST"]),
    Route("/api/login", login, methods=["POST"]),
    Route("/api/me", me),
    Route("/api/palette", get_palette),
    Route("/api/chunk/{cx:int}/{cy:int}", get_chunk),
    Route("/api/place", place, methods=["POST"]),
    Route("/api/pixel/{x:int}/{y:int}", lookup),
    Route("/api/mod/action", mod_action, methods=["POST"]),
    Route("/api/mod/activate", activate_mod, methods=["POST"]),
    Route("/api/clans", list_clans),
    Route("/api/clans/create", create_clan, methods=["POST"]),
    Route("/api/clans/join", join_clan, methods=["POST"]),
    Route("/api/clans/leave", leave_clan, methods=["POST"]),
    Route("/api/earn/start", earn_start, methods=["POST"]),
    Route("/api/earn/submit", earn_submit, methods=["POST"]),
    Route("/api/chat", send_chat, methods=["POST"]),
    Route("/api/chat/{channel}", get_chat),
    Route("/api/online", online),
    Route("/api/info", info),
    WebSocketRoute("/ws", ws_endpoint),
]
middleware = [Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])]
app = Starlette(routes=routes, middleware=middleware)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
