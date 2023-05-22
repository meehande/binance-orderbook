import pytest
from app.listener import Listener
from decimal import Decimal
import numpy as np
from app.snapshot import Snapshotter
from app.manager import OrderBookManager
from app.exceptions import MessageOutOfSyncError, SkipMessage


@pytest.fixture
def order_book_manager():
    return OrderBookManager("BTCUSDT", None, Listener, Snapshotter())


@pytest.fixture
def ordered_stream(sample_stream_message, expected_stream_tob):
    second = {
        "U": sample_stream_message['u'] + 1,
        "u": sample_stream_message['u'] + 5,
        'b': [[Decimal("26971.21000000"), Decimal("0.18092000")],
            [Decimal("26971.16000000"), Decimal("0.00371000")],
            [Decimal("26970.63000000"), Decimal("0.25399000")],],
        'a': [
            [Decimal("26968.99000000"), Decimal("0.01000000")],
            [Decimal("26969.27000000"), Decimal("0.00185000")],
        ]
    }
   
    bids = np.vstack([np.array(second['b']), expected_stream_tob[0]])
    asks = np.vstack([np.array(second['a']), expected_stream_tob[1]])
    second_tob = tuple(bids[np.argmax(bids.T[0])]), tuple(asks[np.argmin(asks.T[0])])
    
    third = {
        "U": sample_stream_message['u'] + 6,
        "u": sample_stream_message['u'] + 10,
        'b': [[Decimal("26971.21000000"), Decimal("0.00000000")],
            [Decimal("26970.63000000"), Decimal("0.24900000")],],
        'a': [
            [Decimal("26968.99000000"), Decimal("0.01000000")],
            [Decimal("26969.27000000"), Decimal("0.00185000")],
        ]

    }
    bids = np.vstack([np.array(third['b']), bids])
    asks = np.vstack([np.array(third['a']), asks])
    third_tob = tuple(bids[np.argmax(bids.T[0])]), tuple(asks[np.argmin(asks.T[0])])

    stream_with_tob = [
        (sample_stream_message, expected_stream_tob),
        (second, second_tob),
        (third, third_tob)
    ]

    return stream_with_tob


@pytest.fixture
def second_snapshot(sample_snapshot, sample_stream_message):
    later_snapshot = (
        sample_stream_message['u'] + 17,
        sample_snapshot[1],
        sample_snapshot[2],
    )

    return later_snapshot


@pytest.fixture
def unordered_stream(sample_stream_message, expected_stream_tob):
    second = {
        "U": sample_stream_message['u'] + 5,
        "u": sample_stream_message['u'] + 10,
        'b': [[Decimal("26971.21000000"), Decimal("0.18092000")],
            [Decimal("26971.16000000"), Decimal("0.00371000")],
            [Decimal("26970.63000000"), Decimal("0.25399000")],],
        'a': [
            [Decimal("26968.99000000"), Decimal("0.01000000")],
            [Decimal("26969.27000000"), Decimal("0.00185000")],
        ]
    }
   
    bids = np.vstack([np.array(second['b']), expected_stream_tob[0]])
    asks = np.vstack([np.array(second['a']), expected_stream_tob[1]])
    second_tob = tuple(bids[np.argmax(bids.T[0])]), tuple(asks[np.argmin(asks.T[0])])
    
    third = {
        "U": sample_stream_message['u'] + 15,
        "u": sample_stream_message['u'] + 19,
        'b': [[Decimal("26971.21000000"), Decimal("0.00000000")],
            [Decimal("26970.63000000"), Decimal("0.24900000")],],
        'a': [
            [Decimal("26968.99000000"), Decimal("0.01000000")],
            [Decimal("26969.27000000"), Decimal("0.00185000")],
        ]

    }
    bids = np.vstack([np.array(third['b']), bids])
    asks = np.vstack([np.array(third['a']), asks])
    third_tob = tuple(bids[np.argmax(bids.T[0])]), tuple(asks[np.argmin(asks.T[0])])


    stream_with_tob = [
        (sample_stream_message, expected_stream_tob),
        (second, second_tob),
        (third, third_tob)
    ]

    return stream_with_tob


@pytest.mark.asyncio
async def test__compare_snapshot(
    mocker, order_book_manager, sample_snapshot, sample_stream_message
):
    snapshot_last_update_id = sample_snapshot[0]

    mock_fetch = mocker.patch.object(
        OrderBookManager, "fetch_snapshot", return_value=sample_snapshot
    )

    message_before_snapshot = {
        **sample_stream_message,
        "U": snapshot_last_update_id - 10,
        "u": snapshot_last_update_id - 5,
    }

    order_book_manager._compare_snapshot(message_before_snapshot)
    with pytest.raises(SkipMessage):
        await order_book_manager._compare_snapshot(message_before_snapshot)

    assert mock_fetch.call_count == 1

    message_after_snapshot = {
        **sample_stream_message,
        "U": snapshot_last_update_id + 5,
        "u": snapshot_last_update_id + 10,
    }

    later_snapshot = (
        snapshot_last_update_id + 7,
        sample_snapshot[1],
        sample_snapshot[2],
    )
    mock_fetch = mocker.patch.object(
        OrderBookManager,
        "fetch_snapshot",
        side_effect=[sample_snapshot, later_snapshot],
    )

    await order_book_manager._compare_snapshot(message_after_snapshot)

    assert order_book_manager._orderbook._last_update_id == snapshot_last_update_id + 7

    assert mock_fetch.call_count == 2

    order_book_manager._snapshot = None
    order_book_manager._last_seen_id = 0
    mock_fetch = mocker.patch.object(
        OrderBookManager, "fetch_snapshot", return_value=sample_snapshot
    )

    await order_book_manager._compare_snapshot(sample_stream_message)

    assert mock_fetch.call_count == 1
    assert order_book_manager._last_seen_id == sample_stream_message["U"] - 1
    assert order_book_manager._orderbook._last_update_id == sample_snapshot[0]
    # todo: test orderbook has same output as snapshot just applied


@pytest.mark.asyncio
async def test__handle_message(
    mocker, order_book_manager, sample_snapshot, sample_stream_message
):
    mocker.patch.object(
        OrderBookManager, "fetch_snapshot", return_value=sample_snapshot
    )

    await order_book_manager._handle_message(sample_stream_message)

    assert order_book_manager._last_seen_id == sample_stream_message["u"]

    unordered_message = {
        **sample_stream_message,
        "U": sample_stream_message["u"] + 3,
        "u": sample_stream_message["u"] + 6,
    }
    with pytest.raises(MessageOutOfSyncError):
        await order_book_manager._handle_message(unordered_message)


@pytest.mark.asyncio
async def test__handle_message_ordered_stream(mocker, order_book_manager, sample_snapshot, ordered_stream):
    """
    patch snapshot + handle_message + buffer
    1. raise out of bounds
    2. raise out of sync
        -> implement what to do in this case too!!
        -> restart?

    3. timeout error
    4. skip message error


    - create sequence of messages in order
    1. orderbook gets updated on each message (2-3 messages)

    same sequence, but out of order
    2. message out of sync raises

    3. all different exceptions raised ?

    """
    mocker.patch.object(
        OrderBookManager, "fetch_snapshot", return_value=sample_snapshot
    )

    for message, expected_tob in ordered_stream:
        await order_book_manager._handle_message(message)
        assert expected_tob[0] == order_book_manager._orderbook.top_bid()
        assert expected_tob[1] == order_book_manager._orderbook.top_ask()

    """
    1.
    tb (Decimal('27095.09000000'), Decimal('11.30847000'))
    ta (Decimal('26970.22000000'), Decimal('9.09544000'))
    2.
    tb: (Decimal('27095.09000000'), Decimal('11.30847000'))
    ta: (Decimal('26968.99000000'), Decimal('0.01000000'))
    3.
    ta: (Decimal('26968.99000000'), Decimal('0.01000000'))
    tb: (Decimal('27095.09000000'), Decimal('11.30847000'))
    """


@pytest.mark.asyncio
async def test__handle_message_unordered_stream(mocker, order_book_manager, sample_snapshot, unordered_stream, second_snapshot):
    mocker.patch.object(
        OrderBookManager, "fetch_snapshot", side_effect=[sample_snapshot, second_snapshot]
    )

    message, expected_tob = unordered_stream[0]
    await order_book_manager._handle_message(message)
    assert expected_tob[0] == order_book_manager._orderbook.top_bid()
    assert expected_tob[1] == order_book_manager._orderbook.top_ask()

    message, _ = unordered_stream[1]
   
    with pytest.raises(MessageOutOfSyncError):
        await order_book_manager._handle_message(message)

    assert expected_tob[0] == order_book_manager._orderbook.top_bid()
    assert expected_tob[1] == order_book_manager._orderbook.top_ask()

    order_book_manager.reset_state()
    
    message, expected_tob = unordered_stream[2]

    await order_book_manager._handle_message(message)
    assert expected_tob[0] == order_book_manager._orderbook.top_bid()
    assert expected_tob[1] == order_book_manager._orderbook.top_ask()


def test__update_orderbook(order_book_manager):
    # str values
    order_book_manager._update_orderbook(
        last_update_id=1,
        bids = [
            ["26971.21000000", "0.18092000"],
            ["26972.02700000", "0.01792000"],
        ],
        asks = [
            ["26969.12300000", "0.28004000"],
            ["26969.87000000", "0.01781000"],
        ]
    )
    assert order_book_manager._orderbook.top_bid() == (Decimal("26972.02700000"), Decimal("0.01792000"))
    assert order_book_manager._orderbook.top_ask() == (Decimal("26969.12300000"), Decimal("0.28004000"))

    # float values -> converted to decimal
    order_book_manager._update_orderbook(
        last_update_id=1,
        bids = [
            [26972.21000000, 0.18092000],
        ],
        asks = [
            [26968.12300000, 0.28004000],
        ]
    )
    
    assert order_book_manager._orderbook.top_bid() == pytest.approx((Decimal("26972.21000000"), Decimal("0.18092000")))
    assert order_book_manager._orderbook.top_ask() == pytest.approx((Decimal("26968.12300000"), Decimal("0.28004000")))

    # remove levels that exist
    order_book_manager._update_orderbook(
        last_update_id=1,
        bids = [
            ["26971.21000000", "0.00000000"],
            ["26969.87000000", "0.01781000"],
        ],
        asks = [
            ["26969.12300000", "0.00000000"],
            ["26969.87000000", "0.01781000"],
        ]
    )

    assert Decimal("26971.21000000") not in order_book_manager._orderbook._bids.keys()
    assert Decimal("26969.12300000") not in order_book_manager._orderbook._asks.keys()

    # remove levels that don't exist
    assert Decimal("30000.21000000") not in order_book_manager._orderbook._bids.keys()
    assert Decimal("30000.12300000") not in order_book_manager._orderbook._asks.keys()

    order_book_manager._update_orderbook(
        last_update_id=1,
        bids = [
            ["30000.21000000", "0.00000000"],
        ],
        asks = [
            ["30000.12300000", "0.00000000"],
        ]
    )

    assert Decimal("30000.21000000") not in order_book_manager._orderbook._bids.keys()
    assert Decimal("30000.12300000") not in order_book_manager._orderbook._asks.keys()

     # only update bids
    order_book_manager._update_orderbook(
        last_update_id=1,
        bids = [
            ["30000.21000000", "0.10000000"],
        ],
        asks = [
        ]

    )

    assert order_book_manager._orderbook._bids[Decimal("30000.21000000")] == Decimal("0.10000000")
    assert Decimal("30000.12300000") not in order_book_manager._orderbook._asks.keys()


