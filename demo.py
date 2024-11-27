import asyncio
import websockets
import json
import aiofiles
import time
from dataclasses import dataclass
import csv
import os
import datetime
from asyncio import Lock
from typing import Optional
# data_lock = Lock()

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

    def to_list(self):
        return [
            self.local_recv_ts_ns, self.exchange, self.base, self.quote,
            self.bid_price, self.bid_qty, self.ask_price, self.ask_qty, self.exg_ts_ms
        ]
    
    def to_csv_string(self):
        return ','.join(map(str, self.to_list()))

def parse_data(data, exchange, base_key, quote_key, bid_key, ask_key, ts_key) -> OrderbookData:
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

def parse_binance_data(data) -> Optional[OrderbookData]:
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

def parse_okx_data(msg) -> Optional[OrderbookData]:
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
   

def log_data(data: OrderbookData):
    file_path = f'booktickers_{str(datetime.datetime.now().date())}.csv'
    file_exists = os.path.isfile(file_path)
    with open(file_path, 'a+', newline='') as f:
        if not file_exists:
            f.write('local_recv_ts_ns,exchange,base,quote,bidprice,bidqty,askprice,askqty,exg_ts_ms\n')
        data_to_write = data.to_csv_string()
        f.write(data_to_write + '\n')
        f.flush()

async def connect_to_exchange(uri, callback, subscribe_message=None):
    async with websockets.connect(uri) as websocket:
        if subscribe_message:
            await websocket.send(json.dumps(subscribe_message))
        async for message in websocket:
            data = json.loads(message)
            parsed_data = callback(data)
            print(parsed_data)
            if parsed_data:
                log_data(parsed_data)

async def connect_to_binance(symbol):
    uri = f"wss://fstream.binance.com/ws/{symbol.lower()}@bookTicker"
    await connect_to_exchange(uri=uri, callback=parse_binance_data, subscribe_message=None)

async def connect_to_okx(inst_id):
    uri = "wss://wsaws.okx.com:8443/ws/v5/public"
    subscribe_message = {
        "op": "subscribe",
        "args": [{"channel": "bbo-tbt", "instId": inst_id}]
    }
    await connect_to_exchange(uri=uri, callback=parse_okx_data, subscribe_message=subscribe_message)

async def main():
    await asyncio.gather(
        connect_to_binance('BTCUSDT'),
        connect_to_okx('BTC-USDT-SWAP')
    )

if __name__ == "__main__":
    # file_path = '/home/ubuntu/arb_check/data.csv'
    # file_exists = os.path.isfile(file_path)
    # print("file exists", file_exists)
    asyncio.run(main())