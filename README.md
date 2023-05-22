Code to publish a live orderbook for listings from Binance. The binance API reference docs are available [here](https://binance-docs.github.io/apidocs/spot/en/#change-log)

# Running the Code

```
pip install -r requirements.txt
export PYTHONPATH=.
python app/main.py
```

This will start an instance of an `OrderBookManager` which will listen for market data updates for `BTCUSDT`, get the current book, and publish the top bid / top ask periodically.

The orderbook implementation follows the guidelines from binance docs, which recommends the following process:

```
## How to manage a local order book correctly
1. Open a stream to wss://stream.binance.com:9443/ws/bnbbtc@depth.

2. Buffer the events you receive from the stream.

3. Get a depth snapshot from https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000 .

4. Drop any event where u is <= lastUpdateId in the snapshot.

5. The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.

6. While listening to the stream, each new event's U should be equal to the previous event's u+1.
-> if not, what? go back to getting a snapshot and acting like a restart

The data in each event is the absolute quantity for a price level.

7. If the quantity is 0, remove the price level.

Receiving an event that removes a price level that is not in your local order book can happen and is normal.


Note: Due to depth snapshots having a limit on the number of price levels, 
a price level outside of the initial snapshot that doesn't have a quantity change 
won't have an update in the Diff. Depth Stream. 
Consequently, those price levels will not be visible in the local order book 
even when applying all updates from the Diff. Depth Stream correctly and cause 
the local order book to have some slight differences with the real order book. 
However, for most use cases the depth limit of 5000 is enough to understand the market and trade effectively.

```

# Tests

```
pip install -r dev-requirements.txt
export PYTHONPATH=.
python -m pytest tests/
```


# TODO
- Restart code
- Websocket exception handling, connection dropping / reconnect logic
- Replace buffer from asyncio.Queue to more robust structure with testing (deal with queue full/processing falling behind producing)
- Add handling for malformatted messages from API
- Make logging adequate for profiling / performance analytics
- Add other exchanges
...

