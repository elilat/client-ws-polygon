import os, asyncio, time, json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from polygon_client import PolygonWebsocketClient

# ─── Load and validate env ────────────────────────────────────
load_dotenv()  # loads .env locally; in prod set real env-vars
API_KEY       = os.getenv("POLYGON_API_KEY", "").strip()  # Added .strip() to remove whitespace
WS_API_KEYS   = set(k.strip() for k in os.getenv("WS_API_KEYS", "").split(",") if k.strip())
ALLOWED_ORIGS = os.getenv("ALLOWED_ORIGINS", "")

print(f"API Key length: {len(API_KEY)}")  # Debug: Check key length (don't print the actual key)
print(f"WS API Keys count: {len(WS_API_KEYS)}")  # Debug: Check number of websocket keys

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
    print("Starting up Polygon client...")
    client = PolygonWebsocketClient(API_KEY, message_queue)
    asyncio.create_task(client.connect())
    app.state.polygon_client = client
    
    print("Starting broadcaster task...")
    asyncio.create_task(broadcaster())

async def broadcaster():
    """
    Pull batches from Polygon, fan-out only trades (ev=="T") to subscribers.
    """
    print("Broadcaster started, waiting for messages...")
    message_count = 0
    last_log_time = time.time()
    
    while True:
        try:
            batch = await message_queue.get()
            current_time = time.time()
            
            # Log stats every 10 seconds
            if current_time - last_log_time > 10:
                print(f"Broadcaster stats: Processed {message_count} messages in the last {current_time - last_log_time:.1f} seconds")
                message_count = 0
                last_log_time = current_time
                
                # Debug: print current subscriptions
                print(f"Active subscriptions: {symbol_subscribers}")
                print(f"Connected clients: {len(user_sockets)}")
            
            for msg in batch:
                message_count += 1
                
                # Debug the message format occasionally
                if message_count % 100 == 0:
                    print(f"Sample message: {msg}")
                
                # Only process trade messages
                if msg.get("ev") != "T":
                    continue
                    
                sym = msg.get("sym")
                if not sym:
                    print(f"Warning: Message has no 'sym' field: {msg}")
                    continue
                
                subs = symbol_subscribers.get(sym, set())
                if not subs:
                    # This can be normal if we just unsubscribed
                    continue
                    
                print(f"Trade for {sym}: {msg.get('p')} - sending to {len(subs)} subscribers")
                
                delivery_count = 0
                for user_id in subs:
                    ws = user_sockets.get(user_id)
                    if ws:
                        try:
                            await ws.send_json(msg)
                            delivery_count += 1
                        except Exception as e:
                            print(f"Error sending to {user_id}: {str(e)}")
                            # Don't clean up here, let the disconnect handler do it
                
                if delivery_count > 0:
                    print(f"Delivered trade for {sym} to {delivery_count}/{len(subs)} subscribers")
                    
        except Exception as e:
            print(f"Error in broadcaster: {str(e)}")
            # Continue processing, don't crash the broadcaster

# ─── WebSocket endpoint ───────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    # 1) Authenticate via ?user_id=…&api_key=…
    user_id = ws.query_params.get("user_id")
    key     = ws.query_params.get("api_key")
    
    if not user_id:
        print("WebSocket connection rejected: No user_id provided")
        await ws.close(code=4401)
        return
        
    if key not in WS_API_KEYS:
        print(f"WebSocket connection rejected: Invalid API key for user {user_id}")
        await ws.close(code=4401)
        return

    await ws.accept()
    print(f"WebSocket connection accepted for user {user_id}")
    
    user_sockets[user_id] = ws
    client_subscriptions[user_id] = set()

    try:
        while True:
            data = await ws.receive_json()
            print(f"Received from {user_id}: {data}")
            
            action = data.get("action")
            symbol = data.get("symbol", "").upper()

            if not symbol:
                print(f"Ignoring message without symbol from {user_id}")
                continue

            if action == "subscribe" and symbol:
                print(f"User {user_id} subscribing to {symbol}")
                symbol_subscribers.setdefault(symbol, set()).add(user_id)
                client_subscriptions[user_id].add(symbol)
                
                # Only subscribe to Polygon if this is the first subscriber
                if len(symbol_subscribers[symbol]) == 1:
                    print(f"First subscriber for {symbol}, subscribing to Polygon")
                    app.state.polygon_client.subscribe(symbol)
                
                # Send confirmation
                await ws.send_json({"status": "subscribed", "symbol": symbol})

            elif action == "unsubscribe" and symbol:
                print(f"User {user_id} unsubscribing from {symbol}")
                subs = symbol_subscribers.get(symbol, set())
                if user_id in subs:
                    subs.remove(user_id)
                    client_subscriptions[user_id].discard(symbol)
                    
                    # Only unsubscribe from Polygon if no subscribers left
                    if not subs:
                        print(f"No subscribers left for {symbol}, unsubscribing from Polygon")
                        app.state.polygon_client.unsubscribe(symbol)
                
                # Send confirmation
                await ws.send_json({"status": "unsubscribed", "symbol": symbol})
            
            else:
                print(f"Unknown action from {user_id}: {action}")
                await ws.send_json({"status": "error", "message": "Unknown action"})

    except WebSocketDisconnect:
        print(f"WebSocket disconnected for user {user_id}")
        # Cleanup on disconnect
        for sym in client_subscriptions.get(user_id, set()):
            subs = symbol_subscribers.get(sym, set())
            subs.discard(user_id)
            if not subs:
                print(f"No subscribers left for {sym} after {user_id} disconnect, unsubscribing from Polygon")
                app.state.polygon_client.unsubscribe(sym)
        client_subscriptions.pop(user_id, None)
        user_sockets.pop(user_id, None)
    
    except Exception as e:
        print(f"Error handling WebSocket for {user_id}: {str(e)}")
        # Ensure cleanup still happens
        for sym in client_subscriptions.get(user_id, set()):
            subs = symbol_subscribers.get(sym, set())
            subs.discard(user_id)
            if not subs:
                app.state.polygon_client.unsubscribe(sym)
        client_subscriptions.pop(user_id, None)
        user_sockets.pop(user_id, None)