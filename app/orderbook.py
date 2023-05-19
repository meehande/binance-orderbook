from collections import OrderedDict
from decimal import Decimal


class OrderBook():
    def __init__(self) -> None:
        # key,value: (price, quantity)
        self._bids:OrderedDict = {}
        self._asks:OrderedDict = {}
        self._last_update_id: int = 0

    def top_ask(self):
        # lowest ask -> key,value: (price, quantity)
        if self._asks:
            price = min(self._asks.keys())
            return price, self._asks[price]
        return 0, 0
    
    def top_bid(self):
        # highest bid
        if self._bids:
            price = max(self._bids.keys())
            return price, self._bids[price]
        return 0, 0

    def remove_bid(self, price:Decimal):
        self._bids.pop(price, None)

    def remove_ask(self, price:Decimal):
        self._asks.pop(price, None)

    def add_or_update_bid(self, price:Decimal, quantity:Decimal):
        self._bids[price] = quantity

    def add_or_update_ask(self, price: Decimal, quantity: Decimal):
        self._asks[price] = quantity

    

