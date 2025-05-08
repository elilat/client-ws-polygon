import os, asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from polygon_client import PolygonWebsocketClient

# ─── Load and validate env ────────────────────────────────────
load_dotenv()  # loads .env locally; in prod set real env-vars
API_KEY       = os.getenv("POLYGON_API_KEY", "")
WS_API_KEYS   = set(os.getenv("WS_API_KEYS", "").split(","))
ALLOWED_ORIGS = os.getenv("ALLOWED_ORIGINS", "")

if not API_KEY or not WS_API_KEYS or not ALLOWED_ORIGS:
    raise RuntimeError("POLYGON_API_KEY, WS_API_KEYS, ALLOWED_ORIGINS must be set")

# ─── FastAPI + CORS ───────────────────────────────────────────
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGS.split(","),
    allow_credentials=True,
    allow_methods=["GET","POST"],
    allow_headers=["*"],
)

# ─── In-memory state ──────────────────────────────────────────
message_queue: asyncio.Queue = asyncio.Queue()
# symbol → set(user_id)
symbol_subscribers: dict[str, set[str]] = {}
# user_id → WebSocket
user_sockets: dict[str, WebSocket] = {}
# user_id → set(symbol)
client_subscriptions: dict[str, set[str]] = {}

# ─── Startup tasks ────────────────────────────────────────────
@app.on_event("startup")
async def on_startup():
    client = PolygonWebsocketClient(API_KEY, message_queue)
    asyncio.create_task(client.connect())
    app.state.polygon_client = client
    asyncio.create_task(broadcaster())

async def broadcaster():
    """
    Pull batches from Polygon, fan-out only trades (ev=="T") to subscribers.
    """
    while True:
        batch = await message_queue.get()
        for msg in batch:
            if msg.get("ev") != "T":
                continue
            sym = msg.get("sym")
            subs = symbol_subscribers.get(sym, set())
            for user_id in subs:
                ws = user_sockets.get(user_id)
                if ws:
                    try:
                        await ws.send_json(msg)
                    except:
                        pass  # cleanup on disconnect

# ─── WebSocket endpoint ───────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    # 1) Authenticate via ?user_id=…&api_key=…
    user_id = ws.query_params.get("user_id")
    key     = ws.query_params.get("api_key")
    if not user_id or key not in WS_API_KEYS:
        await ws.close(code=4401)
        return

    await ws.accept()
    user_sockets[user_id] = ws
    client_subscriptions[user_id] = set()

    try:
        while True:
            data = await ws.receive_json()
            action = data.get("action")
            symbol = data.get("symbol", "").upper()

            if action == "subscribe" and symbol:
                symbol_subscribers.setdefault(symbol, set()).add(user_id)
                client_subscriptions[user_id].add(symbol)
                if len(symbol_subscribers[symbol]) == 1:
                    app.state.polygon_client.subscribe(symbol)

            elif action == "unsubscribe" and symbol:
                subs = symbol_subscribers.get(symbol, set())
                if user_id in subs:
                    subs.remove(user_id)
                    client_subscriptions[user_id].discard(symbol)
                    if not subs:
                        app.state.polygon_client.unsubscribe(symbol)

    except WebSocketDisconnect:
        # Cleanup on disconnect
        for sym in client_subscriptions.get(user_id, set()):
            subs = symbol_subscribers.get(sym, set())
            subs.discard(user_id)
            if not subs:
                app.state.polygon_client.unsubscribe(sym)
        client_subscriptions.pop(user_id, None)
        user_sockets.pop(user_id, None)
