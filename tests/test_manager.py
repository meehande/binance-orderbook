import pytest
from app.listener import Listener
import os
import json
from app.snapshot import Snapshotter
from app.manager import OrderBookManager
from app.exceptions import SnapshotOutOfBoundsError, MessageOutOfSyncError, SkipMessage


@pytest.fixture
def sample_snapshot_message() -> dict:
    fp = os.path.join(os.path.dirname(__file__), 'data/sample-snapshot.json')
    
    with open(fp, 'r') as f:
        data =  json.load(f)
        return data

@pytest.fixture
def sample_snapshot(sample_snapshot_message):
    return  sample_snapshot_message['lastUpdateId'], sample_snapshot_message['bids'], sample_snapshot_message['asks']


@pytest.fixture
def sample_stream_message(sample_snapshot_message):
    snapshot_update_id = sample_snapshot_message['lastUpdateId']
    return {'e': 'depthUpdate', 'E': 1684270515557, 's': 'BTCUSDT', 'U': snapshot_update_id-3, 'u': snapshot_update_id+3, 'b': [['26970.21000000', '0.18092000'], ['26970.16000000', '0.00371000'], ['26970.06000000', '0.00000000'], ['26969.89000000', '0.00000000'], ['26969.63000000', '0.25399000'], ['26969.60000000', '0.20000000'], ['26969.33000000', '0.24243000'], ['26969.29000000', '0.00000000'], ['26969.16000000', '0.26467000'], ['26968.91000000', '0.00000000'], ['26968.80000000', '0.25671000'], ['26968.71000000', '0.31796000'], ['26968.69000000', '0.00000000'], ['26968.64000000', '0.00000000'], ['26968.63000000', '0.00000000'], ['26968.52000000', '0.00000000'], ['26968.33000000', '0.09043000'], ['26968.31000000', '0.00642000'], ['26968.17000000', '0.00000000'], ['26967.93000000', '0.00000000'], ['26967.43000000', '0.00000000'], ['26967.27000000', '0.00069000'], ['26967.24000000', '0.00000000'], ['26967.23000000', '0.00000000'], ['26967.19000000', '0.20000000'], ['26967.03000000', '0.00000000'], ['26967.01000000', '0.00000000'], ['26966.97000000', '0.00000000'], ['26966.72000000', '0.00000000'], ['26966.67000000', '0.23849000'], ['26966.66000000', '0.00000000'], ['26966.63000000', '0.00000000'], ['26966.25000000', '0.28000000'], ['26966.03000000', '0.00741000'], ['26965.58000000', '0.00000000'], ['26965.57000000', '0.00000000'], ['26965.39000000', '2.19499000'], ['26965.38000000', '3.70797000'], ['26965.34000000', '0.00000000'], ['26964.91000000', '0.65955000'], ['26964.76000000', '0.00000000'], ['26964.64000000', '0.00000000'], ['26956.75000000', '2.22551000'], ['26952.16000000', '0.00000000'], ['26951.64000000', '0.00741000'], ['26928.57000000', '0.04149000'], ['26870.21000000', '0.00079000'], ['25621.00000000', '0.08631000'], ['24812.60000000', '0.00000000'], ['16182.12000000', '0.00102000'], ['13485.10000000', '0.00088000'], ['13485.00000000', '0.00000000']], 'a': [['26970.22000000', '9.09544000'], ['26970.25000000', '0.00371000'], ['26970.95000000', '0.00000000'], ['26971.32000000', '0.00000000'], ['26971.89000000', '0.00000000'], ['26972.27000000', '0.00185000'], ['26972.29000000', '0.00000000'], ['26974.71000000', '0.50000000'], ['26975.14000000', '0.09269000'], ['26976.22000000', '0.00000000'], ['26992.48000000', '0.04149000'], ['26994.64000000', '0.28941000'], ['27252.09000000', '0.00000000'], ['29880.00000000', '9.39053000']]}


@pytest.mark.asyncio
async def test__compare_snapshot(mocker, sample_snapshot, sample_stream_message):
    sut = OrderBookManager("BTCUSDT", None, Listener, Snapshotter())
    snapshot_last_update_id = sample_snapshot[0]
    
    mocker.patch.object(OrderBookManager, "fetch_snapshot", return_value=sample_snapshot)
    
    message_before_snapshot = {
        **sample_stream_message,
        'U':  snapshot_last_update_id - 10, 
        'u': snapshot_last_update_id - 5, 
        }
   
    sut._compare_snapshot(message_before_snapshot)
    with pytest.raises(SkipMessage):
        await sut._compare_snapshot(message_before_snapshot)


    message_after_snapshot = {
        **sample_stream_message,
        'U':  snapshot_last_update_id + 5, 
        'u': snapshot_last_update_id + 10, 
        }
    
    """
    TODO: make the snaoshot return different value second time and finish this test
    testcase: if snapshot before message, get new snapshot
    """
   
    mocker.patch.object(OrderBookManager, "fetch_snapshot", return_value=[sample_snapshot])

    sut._compare_snapshot(message_after_snapshot)

    sid = 5000

    """
    testcase:
    - 3. if message encapsulates snapshot, update the orderbook and start regular processing
    - check the orderbook is updated
    - check firstmessage flag updated
    - check lastseenid updated
    """



def test__handle_message():
    """
    - patch snapshot
    - input message in bounds
    - create sequence of messages in order
    1. orderbook gets updated on each message (2-3 messages)

    same sequence, but out of order
    2. message out of sync raises

    """
    pass


def test_process_stream():
    """
    patch snapshot + handle_message + buffer
    1. raise out of bounds
    2. raise out of sync
        -> implement what to do in this case too!!
        -> restart?

    3. timeout error
    4. skip message error    



    
    """




"""
TODO: 
- finish test cases for manager
- write additional restart code
- tests for listener
- readme
- further steps / critiques


"""