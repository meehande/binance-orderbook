import asyncio
import websockets
from loguru import logger
import json


class Listener:
    def __init__(self, symbol: str, buffer: asyncio.Queue) -> None:
        self.symbol = symbol
        self._buffer = buffer
        self._ping_interval = 5 # todo sensible value for this
        self._timeout = 5 # todo sensible value for this

    @property
    def url(self):
        return f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth"
            
    async def run(self):
         logger.debug(f"Connecting to {self.url}")
         async with websockets.connect(self.url, ping_interval=self._ping_interval) as ws:
             while True:
                try:
                    logger.debug(f"connected, waiting for message for {self._timeout} seconds")
                    # ws could be in different states here? if connection closed, need to reconnect...
                    msg = await asyncio.wait_for(ws.recv(), timeout=self._timeout)
                    await self._process_message(msg)

                except asyncio.TimeoutError:
                    logger.warning(f"Timeout error received waiting {self._timeout} seconds for message")
                    continue
                except asyncio.CancelledError:
                    logger.info("Event loop cancelled, exiting")
                    
                    break
                except websockets.ConnectionClosedError:
                    logger.info("Connection closed")
                    # should reconnect here
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    continue
                
    async def _process_message(self, message):
        logger.debug("processing message from websocket")
        parsed = self._parse_message(message)
        if not parsed:
            return
        if self._buffer.full():
            logger.warning("Buffer full, dropping message")
            # todo: this should overwrite / raise exception 
            return
        logger.debug(f"Putting message on buffer: {parsed['U']}")
        await self._buffer.put(parsed)
        

    def _parse_message(self, message):
        try:
            return json.loads(message)
        except ValueError:
            logger.error(f"Unable to parse message: {message}")
            return None
