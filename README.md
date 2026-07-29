# PixelGame

Collaborative pixel canvas (inspired by pixmap.fun / pixelplanet.fun)  
**Stack:** Python FastAPI + MongoDB Atlas + WebSockets + HTML/JS

## Features (MVP)

- Canvas **4096×4096**, coordinates from top-left `0000×0000`
- 50-color palette
- Real-time pixels via WebSocket
- Inventory system (guest 60 / auth 200 + medals)
- Earn mini-game (guess the animal) → +15 pixels
- Cooldown 7s (correct) / 5s (wrong)
- Auth (nickname + password)
- Moderator code activation
- Multi-language chat (Global / RU / EN / AR / TR)
- Settings: grid, brush 1×1 / 3×3 / 5×5, language
- Color voting events every 2 hours
- History snapshot at 14:00 (lives 3 hours)

## Quick start (local)

```bash
# 1. Clone / copy project
cd pixelgame

# 2. Create virtualenv
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# 3. Install
pip install -r requirements.txt

# 4. Configure MongoDB
cp .env.example .env
# Edit .env → put your MongoDB Atlas URI

# 5. Run
python run.py
# Open http://localhost:8000
```

## Render deploy

1. Create Web Service from this repo
2. Set environment variables:
   - `MONGO_URI`
   - `SECRET_KEY`
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn backend.main:app --host 0.0.0.0 --port $PORT`

## Project structure

```
pixelgame/
├── backend/
│   ├── main.py          # FastAPI app + all routes
│   ├── config.py
│   ├── database.py
│   ├── models/
│   └── utils/           # palette, animals
├── frontend/
│   ├── index.html
│   ├── css/style.css
│   └── js/              # canvas, ui, earn, chat, app
├── requirements.txt
├── Dockerfile
└── run.py
```

## Controls

| Key / Action     | Effect              |
|------------------|---------------------|
| Click            | Place pixel         |
| Right-drag / WASD| Pan                 |
| Scroll / Q E     | Zoom                |
| G                | Toggle grid         |

## Next steps

- Clans full UI
- Better animal SVGs
- Admin panel
- History viewer
- Discord OAuth for medal
- Optimise canvas storage (binary chunks)
