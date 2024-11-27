"""
A script to collect and log real-time order book data from Binance and OKX cryptocurrency exchanges.

This script connects to the WebSocket feeds of Binance and OKX exchanges to stream order book data
for BTC/USDT trading pairs. It parses the incoming data into a standardized format and logs it to
a CSV file with timestamps for analysis.

The logged data includes:
- Local receive timestamp (nanoseconds)
- Exchange name
- Trading pair (base/quote)
- Best bid price and quantity
- Best ask price and quantity  
- Exchange timestamp (milliseconds)
"""

import asyncio
import websockets
import json
import time
from dataclasses import dataclass
import os
import datetime
from typing import Optional, List, Any, Dict, Union

@dataclass
class OrderbookData:
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    exchange: str
    local_recv_ts_ns: int
    exg_ts_ms: int
    base: str
    quote: str

    def to_list(self) -> List[Union[int, str, float]]:
        """Convert the OrderbookData to a list format for CSV writing.

        Returns:
            List containing the orderbook data fields in order.
        """
        return [
            self.local_recv_ts_ns, self.exchange, self.base, self.quote,
            self.bid_price, self.bid_qty, self.ask_price, self.ask_qty, self.exg_ts_ms
        ]
    
    def to_csv_string(self) -> str:
        """Convert the OrderbookData to a CSV string.

        Returns:
            Comma-separated string of the orderbook data fields.
        """
        return ','.join(map(str, self.to_list()))

def parse_data(data: Dict[str, Any], exchange: str, base_key: str, quote_key: str, 
               bid_key: str, ask_key: str, ts_key: str) -> OrderbookData:
    """Parse generic exchange data into OrderbookData format.

    Args:
        data: Raw exchange data dictionary
        exchange: Name of the exchange
        base_key: Dictionary key for base currency
        quote_key: Dictionary key for quote currency
        bid_key: Dictionary key for bid data
        ask_key: Dictionary key for ask data
        ts_key: Dictionary key for timestamp

    Returns:
        Parsed OrderbookData object
    """
    return OrderbookData(
        bid_price=float(data[bid_key][0][0]),
        bid_qty=float(data[bid_key][0][1]),
        ask_price=float(data[ask_key][0][0]),
        ask_qty=float(data[ask_key][0][1]),
        exchange=exchange,
        local_recv_ts_ns=int(time.time_ns()),
        exg_ts_ms=int(data[ts_key]),
        base=data[base_key],
        quote=data[quote_key],
    )

def parse_binance_data(data: Dict[str, Any]) -> Optional[OrderbookData]:
    """Parse Binance WebSocket message into OrderbookData format.
    
    Args:
        data: Raw Binance WebSocket message dictionary

    Returns:
        Parsed OrderbookData object if valid data, None otherwise
    """
    if 'b' in data and 'a' in data and 's' in data and 'E' in data:
        return OrderbookData(
            bid_price=float(data['b']),
            bid_qty=float(data['B']),
            ask_price=float(data['a']),
            ask_qty=float(data['A']),
            exchange='binance',
            local_recv_ts_ns=int(time.time_ns()),
            exg_ts_ms=int(data['E']),
            base=data['s'][:-4].upper(),
            quote=data['s'][-4:].upper(),
        )
    else:
        return None

def parse_okx_data(msg: Dict[str, Any]) -> Optional[OrderbookData]:
    """Parse OKX WebSocket message into OrderbookData format.
    
    Args:
        msg: Raw OKX WebSocket message dictionary

    Returns:
        Parsed OrderbookData object if valid data, None otherwise
    """
    if 'data' in msg:
        return OrderbookData(
               bid_price=float(msg['data'][0]['bids'][0][0]),
            bid_qty=float(msg['data'][0]['bids'][0][1]),
            ask_price=float(msg['data'][0]['asks'][0][0]),
            ask_qty=float(msg['data'][0]['asks'][0][1]),
            exchange='okx',
            local_recv_ts_ns=int(time.time_ns()),
            exg_ts_ms=int(msg['data'][0]['ts']),
            base=msg['arg']['instId'].split('-')[0].upper(),
            quote=msg['arg']['instId'].split('-')[-1].upper(),
        )
    else:
        return None
   

def log_data(data: OrderbookData) -> None:
    """Log OrderbookData to a CSV file.
    
    Args:
        data: OrderbookData object to log
    """
    file_path = f'data/booktickers_{str(datetime.datetime.now().date())}.csv'
    file_exists = os.path.isfile(file_path)
    with open(file_path, 'a+', newline='') as f:
        if not file_exists:
            f.write('local_recv_ts_ns,exchange,base,quote,bidprice,bidqty,askprice,askqty,exg_ts_ms\n')
        data_to_write = data.to_csv_string()
        f.write(data_to_write + '\n')
        f.flush()

async def connect_to_exchange(uri: str, callback: callable, subscribe_message: Optional[Dict] = None) -> None:
    """Connect to exchange WebSocket and process messages.
    
    Args:
        uri: WebSocket URI to connect to
        callback: Function to process received messages
        subscribe_message: Optional subscription message to send after connecting
    """
    async with websockets.connect(uri) as websocket:
        if subscribe_message:
            await websocket.send(json.dumps(subscribe_message))
        async for message in websocket:
            data = json.loads(message)
            parsed_data = callback(data)
            if parsed_data:
                log_data(parsed_data)

async def connect_to_binance(symbol: str) -> None:
    """Connect to Binance WebSocket feed.
    
    Args:
        symbol: Trading symbol to subscribe to
    """
    uri = f"wss://fstream.binance.com/ws/{symbol.lower()}@bookTicker"
    await connect_to_exchange(uri=uri, callback=parse_binance_data, subscribe_message=None)

async def connect_to_okx(inst_id: str) -> None:
    """Connect to OKX WebSocket feed.
    
    Args:
        inst_id: Instrument ID to subscribe to
    """
    uri = "wss://wsaws.okx.com:8443/ws/v5/public"
    subscribe_message = {
        "op": "subscribe",
        "args": [{"channel": "bbo-tbt", "instId": inst_id}]
    }
    await connect_to_exchange(uri=uri, callback=parse_okx_data, subscribe_message=subscribe_message)

async def main() -> None:
    """Main async function to run the WebSocket connections."""
    await asyncio.gather(
        connect_to_binance('BTCUSDT'),
        connect_to_okx('BTC-USDT-SWAP')
    )

if __name__ == "__main__":
    asyncio.run(main())