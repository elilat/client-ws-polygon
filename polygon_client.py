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
        self.api_key = api_key
        self.queue = message_queue
        self.url = f"wss://socket.polygon.io/{feed}"
        self.subs = set()
        self.scheduled_subs = set()
        self.schedule_resub = False
        self.max_reconnects = max_reconnects
    
    async def connect(self):
        reconnects = 0
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.load_verify_locations(certifi.where())
        
        while True:
            try:
                async with connect(self.url, ssl=ssl_ctx) as ws:
                    # Send authentication request
                    await ws.send(json.dumps({"action":"auth","params":[self.api_key]}))
                    
                    # First message is connection confirmation
                    connection_msg = json.loads(await ws.recv())
                    print(f"Connection message: {connection_msg}")
                    
                    # For Polygon's API, the 'connected' status is expected for the first message
                    # Now wait for the actual auth response
                    if connection_msg and isinstance(connection_msg, list) and connection_msg[0].get("status") == "connected":
                        try:
                            # Wait for authentication confirmation with a timeout
                            auth_timeout = asyncio.create_task(asyncio.sleep(5))  # 5-second timeout
                            auth_response_task = asyncio.create_task(ws.recv())
                            
                            # Wait for either auth response or timeout
                            done, pending = await asyncio.wait(
                                {auth_response_task, auth_timeout},
                                return_when=asyncio.FIRST_COMPLETED
                            )
                            
                            # Cancel pending tasks
                            for task in pending:
                                task.cancel()
                                
                            # Check if we got the auth response
                            if auth_response_task in done:
                                auth_msg = json.loads(auth_response_task.result())
                                print(f"Auth message: {auth_msg}")
                                
                                # Check for successful authentication
                                if auth_msg and isinstance(auth_msg, list) and auth_msg[0].get("status") in ("success", "auth_success", "authorized"):
                                    print("Authentication successful!")
                                elif connection_msg[0].get("message", "").lower() == "connected successfully":
                                    # Some Polygon API versions might only send the connected message and not a separate auth message
                                    print("Using connection success as auth success")
                                else:
                                    print(f"Authentication failed: {auth_msg}")
                                    raise RuntimeError("Polygon auth failed")
                            else:
                                # If we timed out waiting for auth response, assume the connected message is sufficient
                                print("No explicit auth message received, proceeding with connection")
                        except Exception as e:
                            print(f"Error during authentication phase: {str(e)}")
                            raise
                    else:
                        print(f"Unexpected connection response: {connection_msg}")
                        if isinstance(connection_msg, list) and connection_msg[0].get("status") == "auth_failed":
                            raise RuntimeError(f"Polygon auth failed: {connection_msg[0].get('message', 'No message')}")
                        raise RuntimeError("Unexpected connection response")
                    
                    # Re-subscribe on reconnect
                    await self._resubscribe(ws)
                    
                    # Main message processing loop
                    while True:
                        raw = await ws.recv()
                        data = json.loads(raw)
                        
                        # Only put trade data in the queue, handle status messages separately
                        if isinstance(data, list) and data and data[0].get("ev") != "status":
                            await self.queue.put(data)
                        else:
                            print(f"Status message: {data}")
                            
            except Exception as e:
                print(f"WebSocket error: {str(e)}")
                reconnects += 1
                if reconnects > self.max_reconnects:
                    raise
                # Exponential backoff for reconnection attempts
                backoff_time = min(2**reconnects, 30)
                print(f"Reconnecting in {backoff_time} seconds...")
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