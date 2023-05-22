import pytest
from app.listener import Listener
import os
import json
import pandas as pd
from app.snapshot import Snapshotter
from app.manager import OrderBookManager
from app.exceptions import MessageOutOfSyncError, SkipMessage


@pytest.fixture
def order_book_manager():
    return OrderBookManager("BTCUSDT", None, Listener, Snapshotter())


@pytest.fixture
def ordered_stream(sample_stream_message):
    second = {
        "U": sample_stream_message['u'] + 1,
        "u": sample_stream_message['u'] + 5,
        'b': [["26971.21000000", "0.18092000"],
            ["26971.16000000", "0.00371000"],
            ["26970.63000000", "0.25399000"],],
        'a': [
            ["26968.99000000", "0.01000000"],
            ["26969.27000000", "0.00185000"],
        ]

    }
    third = {
        "U": sample_stream_message['u'] + 6,
        "u": sample_stream_message['u'] + 10,
        'b': [["26971.21000000", "0.00000000"],
            ["26970.63000000", "0.24900000"],],
        'a': [
            ["26968.99000000", "0.01000000"],
            ["26969.27000000", "0.00185000"],
        ]

    }
    return [sample_stream_message, second, third]



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

    # todo: check ob has top bid/ask incorporating the streamed messasge

    unordered_message = {
        **sample_stream_message,
        "U": sample_stream_message["u"] + 3,
        "u": sample_stream_message["u"] + 6,
    }
    with pytest.raises(MessageOutOfSyncError):
        await order_book_manager._handle_message(unordered_message)


@pytest.mark.asyncio
async def test_process_stream(mocker, ordered_stream):
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
    #todo: mock async queue / add the items to the queue?
    for message in ordered_stream:
        await order_book_manager._buffer.put(message)
        # todo: how to make it read three and then stop

    mock_buffer = mocker.object.patch(OrderBookManager, )


def test__update_orderbook():
    """
    1. removes levels 
    2. adds levels
    3. only bids
    4. only asks
    5. remove levels that don't exist
    """
    pass

"""
TODO: 
- finish test cases for manager
- write additional restart code
- tests for listener
- readme

- fix how orderman instantiated (listener + snapshotter params)

- further steps / critiques
    -> proper managed structure for the buffer (deal with fullness etc)

- check logging is good
-> can do timing 
-> request id
-> parsable for audit ??
-> levelled appropriately


- malformatted data -> exception for this / how to handle
"""
