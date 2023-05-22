import asyncio
import websockets
from app.log_handler import logger
import json


class Listener:
    def __init__(self, symbol: str, buffer: asyncio.Queue) -> None:
        self.symbol = symbol
        self._buffer = buffer
        self._ping_interval = 5 # todo sensible value for this (seconds)
        self._timeout = 5 # todo sensible value for this (seconds)

    @property
    def url(self):
        return f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth"
            
    async def run(self):
         logger.debug(f"<blue>connecting to {self.url}</blue>")
         async with websockets.connect(self.url, ping_interval=self._ping_interval) as ws:
             while True:
                try:
                    logger.debug(f"<blue>waiting for message from ws</blue>")
                    # ws could be in different states here? if connection closed, need to reconnect...
                    msg = await asyncio.wait_for(ws.recv(), timeout=self._timeout)
                    await self._process_message(msg)

                except asyncio.TimeoutError:
                    logger.warning(f"<blue>timeout error received waiting {self._timeout} seconds for message</blue>")
                    continue
                except asyncio.CancelledError:
                    logger.info("<blue>Event loop cancelled, exiting</blue>")
                    
                    break
                except websockets.ConnectionClosedError:
                    logger.info("<blue>Connection closed</blue>")
                    # todo: should reconnect here
                except Exception as e:
                    logger.error(f"<blue>Unexpected error: {e}</blue>")
                    continue
                
    async def _process_message(self, message):
        logger.debug("<blue>parse incoming message</blue>")
        parsed = self._parse_message(message)
        if not parsed:
            return
        if self._buffer.full():
            logger.warning("<blue>buffer full, dropping message</blue>")
            # todo: this should overwrite / raise exception 
            return
        logger.debug(f"<blue>putting message {parsed['U']} on buffer</blue>")
        await self._buffer.put(parsed)
        

    def _parse_message(self, message):
        try:
            return json.loads(message)
        except ValueError:
            logger.error(f"<blue>Unable to parse message: {message}</blue>")
            return None
