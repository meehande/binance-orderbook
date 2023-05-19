import pytest

from app.orderbook import OrderBook
from decimal import Decimal


@pytest.fixture
def top_bid():
    return (Decimal(10.897), Decimal(10))


@pytest.fixture
def bids(top_bid):
    return [(Decimal(9.72),Decimal(8)), top_bid, (Decimal(10.003),Decimal(10))]


@pytest.fixture
def top_ask():
    return (Decimal(9.054), Decimal(6))



@pytest.fixture
def asks(top_ask):
    return [(Decimal(9.453),Decimal(7)), top_ask, (Decimal(9.234),Decimal(10))]


@pytest.fixture
def orderbook(bids, asks):
    ob = OrderBook()
    for bid in bids:
        ob.add_or_update_bid(*bid)

    for ask in asks:
        ob.add_or_update_ask(*ask)

    return ob


def test_top_bid(orderbook: OrderBook, top_bid: Decimal):
    assert orderbook.top_bid() == top_bid, f"Top bid should be {top_bid}"


def test_top_ask(orderbook: OrderBook, top_ask: Decimal):
    assert orderbook.top_ask() == top_ask, f"Top ask should be {top_ask}"