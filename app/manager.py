import asyncio
import datetime as dt
from decimal import Decimal
from loguru import logger
from app.listener import Listener
from app.snapshot import Snapshotter
from app.orderbook import OrderBook
from app.exceptions import SnapshotOutOfBoundsError, MessageOutOfSyncError, SkipMessage


class OrderBookManager:
    def __init__(self, symbol: str, buffer, listener_type: Listener, snapshotter: Snapshotter) -> None:
        self._symbol = symbol
        self._orderbook = OrderBook()

        self._buffer = buffer
        self._listener = listener_type(symbol, self._buffer)
        self._snapshotter = snapshotter
        self._snapshot = None
        self._period = 5
        self._is_first_message = True
        self._last_update_id = 0
        self._last_seen_id = 0

    async def run(self):
        stream_task = asyncio.create_task(self.buffer_stream())
        update_orderbook_task = asyncio.create_task(self.process_stream())
        publish_orderbook = asyncio.create_task(self.publish_orderbook())
        await asyncio.gather(stream_task, update_orderbook_task, publish_orderbook)

    async def buffer_stream(self):
        await self._listener.run()


    async def fetch_snapshot(self):
        return await self._snapshotter.fetch_latest(self._symbol)


    async def publish_orderbook(self):
        logger.info(f"TS, Top Bid, Top Ask")
        while True:
            try:
                logger.info(f"{dt.datetime.utcnow()}, {self._orderbook.top_bid()}, {self._orderbook.top_ask()}")
                await asyncio.sleep(self._period)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                continue

    async def process_stream(self):
        # messages have been buffered
        while True:
            try:
                logger.debug(f"waiting for message from queue")
                message = await self._buffer.get()
                logger.debug(f"Received message from queue: {message['U']}")
                await self._handle_message(message) 
            except SnapshotOutOfBoundsError:
                logger.debug(f"Snapshot out of bounds, resetting")
                self._snapshot = None 

            except MessageOutOfSyncError:
                logger.debug(f"Message out of sync... restart with new snapshot??")
                
            except asyncio.TimeoutError:
                logger.warning(f"Timeout error received waiting for message from queue")
                continue
            except asyncio.CancelledError:
                logger.info("Event loop cancelled from manager exiting")
                break
            except SkipMessage:
                logger.info("Skipping message")
                continue

            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                continue

    async def _handle_message(self, message):

        logger.debug(f"Processing {message['U']} - {message['u']}")
        
        if self._is_first_message:
            logger.debug(f"First message, checking snapshot")
            await self._compare_snapshot(message)

        if message['U'] != self._last_seen_id + 1:
            logger.warning(f"message is not next in sequence! Received {message['U']}, expected {self._last_seen_id + 1}")
            raise MessageOutOfSyncError()
        
        self._update_orderbook(message['u'], message['b'], message['a'])
        self._last_seen_id = message['u']


    async def _compare_snapshot(self, message):
        if self._snapshot is None:
            self._snapshot = await self.fetch_snapshot()
        
        last_update_id, bids, asks = self._snapshot

        U = message['U'] # id of first update
        u = message['u'] # id of last update
        
        if U > last_update_id:
            logger.debug(f"Snapshot before stream, refetch: U:{U}, u:{u}, last_update_id:{last_update_id}")
            self._snapshot = None
            await self._compare_snapshot(message)
            # todo: have a counter for this, if it happens too many times, skip message

        elif u < last_update_id:
            logger.debug(f"Snapshot after message, skipping:  U:{U}, u:{u}, last_update_id:{last_update_id}")
            raise SkipMessage()
        
        elif( U <= last_update_id + 1) and (u >= last_update_id + 1):
            logger.debug(f'Snapshot fits, processing message. setting last seen to {U - 1}')
            self._is_first_message = False
            self._last_seen_id = U - 1
            self._update_orderbook(last_update_id, bids, asks)
            return
        
        logger.warning(f"Unexpected scenario - U:{U}, u:{u}, last_update_id:{last_update_id}")
        raise NotImplementedError(f"Unexpected - U:{U}, u:{u}, last_update_id:{last_update_id}")

    def _update_orderbook(self, last_update_id, bids, asks):
        for bid in bids:
            # price, quantity
            price, quantity = [Decimal(x) for x in bid]
            if quantity == 0:
                self._orderbook.remove_bid(price)
            else:
                self._orderbook.add_or_update_bid(price, quantity)

        for ask in asks:
            # price, quantity
            price, quantity = [Decimal(x) for x in ask]
            if quantity == 0:
                self._orderbook.remove_ask(price)
            else:
                self._orderbook.add_or_update_ask(price, quantity)
        
        self._orderbook._last_update_id = last_update_id
       

        """
    From the binance docs: 
    How to manage a local order book correctly

1. Open a stream to wss://stream.binance.com:9443/ws/bnbbtc@depth.

2. Buffer the events you receive from the stream.

3. Get a depth snapshot from https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000 .

4. Drop any event where u is <= lastUpdateId in the snapshot.

5. The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.

6. While listening to the stream, each new event's U should be equal to the previous event's u+1.
-> if not, what? go back to getting a snapshot and acting like a restart

The data in each event is the absolute quantity for a price level.

7. If the quantity is 0, remove the price level.

Receiving an event that removes a price level that is not in your local order book can happen and is normal.
    """




    