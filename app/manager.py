import asyncio
import datetime as dt
from decimal import Decimal
from app.log_handler import logger
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
        logger.debug(f"<green>fetching snapshot</green>")
        
        return await self._snapshotter.fetch_latest(self._symbol)
    
    def reset_state(self):
        # todo: should the orderbook be reset or no..?
        self._orderbook = OrderBook()
        self._snapshot = None
        self._last_seen_id = 0
        self._is_first_message = True

    async def publish_orderbook(self):
        while True:
            try:
                bid = self._orderbook.top_bid()
                ask = self._orderbook.top_ask()
                timestamp = dt.datetime.utcnow()
                
                top_bid = f"<red> {{'timestamp': {timestamp}, 'side': 'bid' 'price': {bid[0]}, 'quantity': {bid[1]} }} </red>"
                top_ask= f"<red> {{'timestamp': {timestamp}, 'side': 'ask' 'price': {ask[0]}, 'quantity': {ask[1]} }} </red>"

                logger.info(top_bid)
                logger.info(top_ask)
                await asyncio.sleep(self._period)
            except Exception as e:
                logger.error(f"<red>Unexpected error: {e}</red>")
                continue

    async def process_stream(self):
        # messages have been buffered from listener
        while True:
            try:
                logger.debug(f"<green>waiting for message from queue</green>")
                message = await self._buffer.get()
                logger.debug(f"<green>Received message from queue: {message['U']}</green>")
                await self._handle_message(message) 

            except MessageOutOfSyncError:
                logger.debug(f"<green>Message out of sync... restart with new snapshot??</green>")
                self.reset_state()
                # todo: reprocess the message / put back onto queue at front

            except asyncio.TimeoutError:
                logger.warning(f"<green>Timeout error received waiting for message from queue</green>")
                continue
            except asyncio.CancelledError:
                logger.info("<green>Event loop cancelled from manager exiting</green>")
                break
            except SkipMessage:
                logger.info("<green>Skipping message</green>")
                continue

            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                continue

    async def _handle_message(self, message):

        logger.debug(f"<green>Processing {message['U']} - {message['u']}</green>")
        
        if self._is_first_message:
            await self._compare_snapshot(message)

        if message['U'] != self._last_seen_id + 1:
            logger.warning(f"<green>message is not next in sequence! Received {message['U']}, expected {self._last_seen_id + 1}</green>")
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
            logger.debug(f"<green>Snapshot before stream, refetch: U:{U}, u:{u}, last_update_id:{last_update_id}</green>")
            self._snapshot = None
            await self._compare_snapshot(message)
            return
            # todo: have a counter for this, if it happens too many times, skip message

        elif u < last_update_id:
            logger.debug(f"<green>Message before snapshot, skipping:  U:{U}, u:{u}, last_update_id:{last_update_id}</green>")
            raise SkipMessage()
        
        elif( U <= last_update_id + 1) and (u >= last_update_id + 1):
            logger.debug(f'<green>Snapshot fits, processing message. setting last seen to {U - 1}</green>')
            self._is_first_message = False
            self._last_seen_id = U - 1
            self._update_orderbook(last_update_id, bids, asks)
            return
        
        logger.warning(f"<green>Unexpected scenario - U:{U}, u:{u}, last_update_id:{last_update_id}</green>")
       
        raise NotImplementedError(f"<green>Unexpected - U:{U}, u:{u}, last_update_id:{last_update_id}</green>")

    def _update_orderbook(self, last_update_id: int, bids: Iterable[Iterable[Union[str, int]]], asks: Iterable[Iterable[Union[str, int]]]):
        logger.debug(f"<green>Updating orderbook with {last_update_id}</green>")
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
        logger.debug(f"<green>orderbook updated to {last_update_id}</green>")
       



    