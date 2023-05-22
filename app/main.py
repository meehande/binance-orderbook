
import asyncio
from app.listener import Listener
from app.manager import OrderBookManager
from app.snapshot import Snapshotter


if __name__ == "__main__":
    symbol = "BTCUSDT"
    buffer = asyncio.Queue()

    man = OrderBookManager(symbol, buffer, Listener(symbol, buffer), Snapshotter())
    asyncio.get_event_loop().run_until_complete(man.run())
