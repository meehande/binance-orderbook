import asyncio
import datetime as dt
from decimal import Decimal
from loguru import logger
from typing import Iterable, Union
from app.listener import Listener
from app.snapshot import Snapshotter
from app.orderbook import OrderBook
from app.exceptions import MessageOutOfSyncError, SkipMessage


class OrderBookManager:
    def __init__(self, symbol: str, buffer, listener: Listener, snapshotter: Snapshotter) -> None:
        self._symbol = symbol
        self._orderbook = OrderBook()

        self._buffer = buffer
        self._listener = listener
        self._snapshotter = snapshotter
        self._snapshot = None
        self._period = 5
        self._is_first_message = True
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
    
    def reset_state(self):
        # todo: should the orderbook be reset or no..?
        self._orderbook = OrderBook()
        self._snapshot = None
        self._last_seen_id = 0
        self._is_first_message = True

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
        # messages have been buffered from listener
        while True:
            try:
                logger.debug(f"waiting for message from queue")
                message = await self._buffer.get()
                logger.debug(f"Received message from queue: {message['U']}")
                await self._handle_message(message) 

            except MessageOutOfSyncError:
                import pdb; pdb.set_trace()
                logger.debug(f"Message out of sync... restart with new snapshot??")
                self.reset_state()
                # todo: reprocess the message / put back onto queue at front

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
            return
            # todo: have a counter for this, if it happens too many times, skip message

        elif u < last_update_id:
            logger.debug(f"Message before snapshot, skipping:  U:{U}, u:{u}, last_update_id:{last_update_id}")
            raise SkipMessage()
        
        elif( U <= last_update_id + 1) and (u >= last_update_id + 1):
            logger.debug(f'Snapshot fits, processing message. setting last seen to {U - 1}')
            self._is_first_message = False
            self._last_seen_id = U - 1
            self._update_orderbook(last_update_id, bids, asks)
            return
        
        logger.warning(f"Unexpected scenario - U:{U}, u:{u}, last_update_id:{last_update_id}")
       
        raise NotImplementedError(f"Unexpected - U:{U}, u:{u}, last_update_id:{last_update_id}")

    def _update_orderbook(self, last_update_id: int, bids: Iterable[Iterable[Union[str, int]]], asks: Iterable[Iterable[Union[str, int]]]):
        for bid in bids:
            
            price, quantity = [Decimal(x) for x in bid]
            
            if quantity == 0:
                self._orderbook.remove_bid(price)
            else:
                self._orderbook.add_or_update_bid(price, quantity)

        for ask in asks:
            price, quantity = [Decimal(x) for x in ask]

            if quantity == 0:
                self._orderbook.remove_ask(price)
            else:
                self._orderbook.add_or_update_ask(price, quantity)
        
        self._orderbook._last_update_id = last_update_id
       



    