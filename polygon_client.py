# polygon_client.py
import ssl, json, asyncio, os, datetime
from websockets import connect
import certifi

class PolygonWebsocketClient:
    """
    Single authenticated WS→Polygon.
    Pushes each incoming list of messages into `message_queue`.
    """

    def __init__(self, api_key: str, message_queue: asyncio.Queue,
                 feed: str = "stocks", max_reconnects: int = 5):
        self.api_key = api_key.strip()
        if not self.api_key:
            raise ValueError("API key cannot be empty")

        self.queue = message_queue
        self.url = f"wss://socket.polygon.io/{feed}"
        self.subs = set()             # actually-subscribed topics
        self.scheduled_subs = set()   # for resubscribe on reconnect
        self.ws = None                # live websocket
        self.max_reconnects = max_reconnects

        # Create logs directory if it doesn't exist
        self.logs_dir = "polygon_logs"
        os.makedirs(self.logs_dir, exist_ok=True)

        print(f"Polygon client initialized with API key length: {len(self.api_key)}")

    def _log_response(self, data, message_type="message"):
        """Log Polygon responses to a file with timestamp."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
        log_file = os.path.join(self.logs_dir, f"polygon_{timestamp}.log")
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with open(log_file, "a") as f:
            f.write(f"[{current_time}] {message_type}: {json.dumps(data)}\n")

    async def connect(self):
        reconnects = 0
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.load_verify_locations(certifi.where())

        while True:
            try:
                print(f"Attempting to connect to {self.url}...")
                async with connect(self.url, ssl=ssl_ctx) as ws:
                    self.ws = ws
                    # Authenticate
                    auth_payload = {"action": "auth", "params": self.api_key}
                    print(f"Sending auth request with key length: {len(self.api_key)}")
                    await ws.send(json.dumps(auth_payload))

                    # Connection confirmation
                    connection_msg = json.loads(await ws.recv())
                    print(f"Connection message: {connection_msg}")
                    self._log_response(connection_msg, "connection")

                    # Auth response
                    auth_msg = json.loads(await ws.recv())
                    print(f"Auth message: {auth_msg}")
                    self._log_response(auth_msg, "authentication")

                    if (isinstance(auth_msg, list) and auth_msg[0].get("status") == "auth_failed"):
                        raise RuntimeError(f"Polygon auth failed: {auth_msg[0].get('message')}")

                    print("Authentication successful!")

                    # Resubscribe any existing topics (after a reconnect)
                    await self._resubscribe(ws)

                    print("Entering main message loop...")
                    while True:
                        raw = await ws.recv()
                        data = json.loads(raw)
                        # log + stdout print
                        self._log_response(data, "data")
                        print("⬅️ Polygon sent:", data)

                        if isinstance(data, list) and data:
                            if data[0].get("ev") != "status":
                                await self.queue.put(data)

            except Exception as e:
                print(f"WebSocket error: {str(e)}")
                self._log_response({"error": str(e)}, "error")
                reconnects += 1
                if reconnects > self.max_reconnects:
                    print(f"Maximum reconnect attempts ({self.max_reconnects}) reached. Giving up.")
                    raise
                backoff = min(2**reconnects, 30)
                print(f"Reconnecting in {backoff}s (attempt {reconnects}/{self.max_reconnects})…")
                await asyncio.sleep(backoff)

    def subscribe(self, symbol: str):
        topic = f"T.{symbol.upper()}"
        if topic in self.subs:
            return
        self.subs.add(topic)
        self.scheduled_subs.add(topic)

        # Send immediately if WS is live
        if self.ws and not getattr(self.ws, "closed", False):
            payload = {"action": "subscribe", "params": topic}
            asyncio.create_task(self.ws.send(json.dumps(payload)))
            self._log_response(payload, "subscribe_request")

    def unsubscribe(self, symbol: str):
        topic = f"T.{symbol.upper()}"
        if topic not in self.subs:
            return
        self.subs.remove(topic)
        self.scheduled_subs.discard(topic)

        if self.ws and not getattr(self.ws, "closed", False):
            payload = {"action": "unsubscribe", "params": topic}
            asyncio.create_task(self.ws.send(json.dumps(payload)))
            self._log_response(payload, "unsubscribe_request")

    async def _resubscribe(self, ws):
        """On reconnect: subscribe/unsubscribe any topics from `scheduled_subs`."""
        to_add = self.scheduled_subs - self.subs
        to_rem = self.subs - self.scheduled_subs

        if to_add:
            print(f"Re-subscribing to: {to_add}")
            params = ",".join(to_add)
            payload = {"action": "subscribe", "params": params}
            await ws.send(json.dumps(payload))
            self._log_response(payload, "subscribe_request")
            self.subs.update(to_add)

        if to_rem:
            print(f"Re-unsubscribing from: {to_rem}")
            params = ",".join(to_rem)
            payload = {"action": "unsubscribe", "params": params}
            await ws.send(json.dumps(payload))
            self._log_response(payload, "unsubscribe_request")
            self.subs.difference_update(to_rem)
