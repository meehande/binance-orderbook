import aiohttp
from loguru import logger
import requests 


class Snapshotter():
    def format_url(self, symbol: str):
        return f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}&limit=1000"

    async def fetch_latest(self,  symbol: str):
        logger.debug(f"Fetching latest snapshot for {symbol}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.format_url(symbol)) as resp:
                    resp.raise_for_status()
                    latest = await resp.json()
                    logger.debug(f"Latest snapshot for {symbol} fetched")
                    return self._extract_snapshot(latest)
        except Exception as e:
            logger.error(f"Error fetching latest snapshot: {e}")
            raise e
            
            
    def sync_fetch_latest(self, symbol: str):
        logger.debug(f"Fetching latest snapshot for {symbol}")
        resp = requests.get(self.format_url(symbol))
        latest = resp.json()

        # todo: exception handling
        return self._extract_snapshot(latest)
            
    def _extract_snapshot(self, latest):
        """
        Response:
        {
        "lastUpdateId": 1027024,
        "bids": [
            [
            "4.00000000",     // PRICE
            "431.00000000"    // QTY
            ]
        ],
        "asks": [
            [
            "4.00000200",
            "12.00000000"
            ]
        ]
        }
        
        """
        last_update_id = int(latest['lastUpdateId'])
        bids = latest['bids']
        asks = latest['asks']
        return last_update_id, bids, asks