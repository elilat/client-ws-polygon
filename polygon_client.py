import ssl, json, asyncio
from websockets import connect
import certifi

class PolygonWebsocketClient:
    """
    Single authenticated WS→Polygon.
    Pushes each incoming list of messages into `message_queue`.
    """
    def __init__(self, api_key: str, message_queue: asyncio.Queue,
                 feed: str = "stocks", max_reconnects: int = 5):
        self.api_key = api_key.strip()  # Ensure no whitespace
        if not self.api_key:
            raise ValueError("API key cannot be empty")
            
        self.queue = message_queue
        self.url = f"wss://socket.polygon.io/{feed}"
        self.subs = set()
        self.scheduled_subs = set()
        self.schedule_resub = False
        self.max_reconnects = max_reconnects
        print(f"Polygon client initialized with API key length: {len(self.api_key)}")
    
    async def connect(self):
        reconnects = 0
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.load_verify_locations(certifi.where())
        
        while True:
            try:
                print(f"Attempting to connect to {self.url}...")
                async with connect(self.url, ssl=ssl_ctx) as ws:
                    # Send authentication request
                    auth_payload = {"action":"auth","params":[self.api_key]}
                    print(f"Sending auth request with key length: {len(self.api_key)}")
                    await ws.send(json.dumps(auth_payload))
                    
                    # First message is connection confirmation
                    connection_msg = json.loads(await ws.recv())
                    print(f"Connection message: {connection_msg}")
                    
                    # Wait for authentication message
                    auth_msg = json.loads(await ws.recv())
                    print(f"Auth message: {auth_msg}")
                    
                    # Check for failed authentication
                    if (isinstance(auth_msg, list) and 
                        auth_msg[0].get("status") == "auth_failed"):
                        error_msg = auth_msg[0].get("message", "Unknown error")
                        print(f"Authentication explicitly failed: {error_msg}")
                        
                        # If we keep failing, suggest API key issues
                        if reconnects >= 2:
                            print("CRITICAL: Multiple authentication failures.")
                            print("Please verify your Polygon API key is correct and has WebSocket permissions.")
                            print("Suggestion: Check if your account subscription allows WebSocket access.")
                        
                        raise RuntimeError(f"Polygon auth failed: {error_msg}")
                    
                    # Check for successful authentication
                    if (isinstance(auth_msg, list) and 
                        auth_msg[0].get("status") in ("success", "auth_success", "authorized")):
                        print("Authentication successful!")
                    else:
                        print(f"Unexpected authentication response: {auth_msg}")
                        raise RuntimeError("Unexpected authentication response")
                    
                    # Re-subscribe on reconnect
                    await self._resubscribe(ws)
                    
                    # Main message processing loop
                    print("Entering main message loop...")
                    while True:
                        raw = await ws.recv()
                        data = json.loads(raw)
                        
                        # Process message based on type
                        if isinstance(data, list) and data:
                            if data[0].get("ev") == "status":
                                print(f"Status message: {data}")
                            else:
                                await self.queue.put(data)
                        else:
                            print(f"Unexpected message format: {data}")
                            
            except Exception as e:
                print(f"WebSocket error: {str(e)}")
                reconnects += 1
                if reconnects > self.max_reconnects:
                    print(f"Maximum reconnect attempts ({self.max_reconnects}) reached. Giving up.")
                    raise
                
                # Exponential backoff for reconnection attempts
                backoff_time = min(2**reconnects, 30)
                print(f"Reconnecting in {backoff_time} seconds... (Attempt {reconnects}/{self.max_reconnects})")
                await asyncio.sleep(backoff_time)
    
    def subscribe(self, symbol: str):
        topic = f"T.{symbol.upper()}"
        self.scheduled_subs.add(topic)
        self.schedule_resub = True
    
    def unsubscribe(self, symbol: str):
        topic = f"T.{symbol.upper()}"
        self.scheduled_subs.discard(topic)
        self.schedule_resub = True
    
    async def _resubscribe(self, ws):
        if not self.schedule_resub:
            return
        
        to_add = self.scheduled_subs - self.subs
        to_rem = self.subs - self.scheduled_subs
        
        if to_add:
            print(f"Subscribing to: {to_add}")
            await ws.send(json.dumps({"action":"subscribe","params":list(to_add)}))
        
        if to_rem:
            print(f"Unsubscribing from: {to_rem}")
            await ws.send(json.dumps({"action":"unsubscribe","params":list(to_rem)}))
        
        self.subs = set(self.scheduled_subs)
        self.schedule_resub = False