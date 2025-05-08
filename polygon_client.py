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
                    # Authenticate
                    await ws.send(json.dumps({"action":"auth","params":self.api_key}))
                    auth = json.loads(await ws.recv())
                    if auth[0].get("status","") not in ("authorized","auth_success"):
                        raise RuntimeError("Polygon auth failed")

                    # Re-subscribe on reconnect
                    await self._resubscribe(ws)

                    # Read loop
                    while True:
                        raw = await ws.recv()
                        data = json.loads(raw)
                        await self.queue.put(data)

            except Exception:
                reconnects += 1
                if reconnects > self.max_reconnects:
                    raise
                await asyncio.sleep(min(2**reconnects, 30))

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
            await ws.send(json.dumps({"action":"subscribe","params":",".join(to_add)}))
        if to_rem:
            await ws.send(json.dumps({"action":"unsubscribe","params":",".join(to_rem)}))
        self.subs = set(self.scheduled_subs)
        self.schedule_resub = False
