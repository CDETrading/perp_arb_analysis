import time
import json
import queue
import gzip
import numpy as np
import pandas as pd
import logging
import threading
import websockets
import asyncio 


async def ws_handler(exchange_id, url, msg, collective_data, start_event):
    print(f"{exchange_id} thread is ready to start")
    start_event.wait()  # hold for start singal
    
    print(f"start ! {exchange_id}" )
    start_time = time.time()

    while True:
        try :
            async with websockets.connect(url, ping_interval=10) as websocket:
                # Send a message
                await websocket.send(json.dumps(msg))
                logging.info(f"Sent: {msg}")
                
                while True :
                    # Receive a response
                    recv = await websocket.recv()
                    ts = time.time_ns()  # universial local ts
                    #response_ = Response(id, ts)
                    current_time = time.time()  # for ping pong
                    
                    if isinstance(recv, bytes) :
                        # for binary data
                        decompress_data = gzip.decompress(recv).decode("utf-8") # parse to string
                        print(f"msg is for {exchange_id}, {recv}")
                       
                        # response_ .data = decompress_data
                        # collective_data.put(response_)
                        #response = json.loads(decompress_data)  # !!!
                        #print(response)
                        
                        if decompress_data.find("ping") > 0:
                            pong = decompress_data[(decompress_data.find("ping")+6) :  decompress_data.find(',')]
                            logging.info(f"pong : {pong} for {id} " )     
                            # Create and send the pong response
                            pong_message = {"pong": int(pong)}
                            await websocket.send(json.dumps(pong_message))

                    else :
                        # other exchanges
                        print(f"msg is for {exchange_id}, {recv}")
                        
                        #response_.data = recv  # raw msg
                        #collective_data.put(response_)  # tuck into queue

                    if current_time - start_time >= 60 :   # 1 mins
                       # ping-pong
                        if id == "gateio" :
                            ping_msg = {"time" : f"{int(current_time)}", "channel" : "futures.ping"}
                            logging.info(f"Sent: {ping_msg}")
                            await websocket.send(json.dumps(ping_msg))
                            
                        elif id == "bybit" : 
                            ping_msg = { "op": "ping"}
                            logging.info(f"Sent: {ping_msg}")
                            await websocket.send(json.dumps(ping_msg))

                        start_time = current_time  # update ping-pong start time
                        
                                    
        except websockets.exceptions.ConnectionClosedError as e:
            logging.info(f"reconnect for {exchange_id} err msg is {e}") 
            await asyncio.sleep(1)
        
        except websockets.exceptions.ConnectionClosedOK:
            logging.info("Connection closed gracefully (1001). Retrying...")
            await asyncio.sleep(1)

        except asyncio.TimeoutError as e:
            logging.info(f"Timeout occurred for {exchange_id}, err msg is {e} , reconnecting ")
            await asyncio.sleep(1)  
            
                

def send_websocket_request(exchange_id, ws_url, message, df, start_event):
    asyncio.run(ws_handler(exchange_id, ws_url, message, df, start_event))


def quote_exchange(url, msg, data_queue) : 

    pass

def trade() :
    pass


if __name__ == "__main__" :
    
    record = {}  # to record detailed trading record
    target_currency = "XRP"
    base_currency = "USDT"
    exchanges = ["gateio", "htx"]
    urls = [  
            "wss://api.hbdm.com/linear-swap-ws",
            "wss://fx-ws.gateio.ws/v4/ws/usdt",
            ] 
    ts = time.time_ns()  # for subscribe
    msgs = [
            {"sub" :f"market.{target_currency}-{base_currency}.depth.step0", "id" : "test0"},
            {"time" : ts, "channel" : "futures.book_ticker", "event" : "subscribe", "payload" : [f"{target_currency}_{base_currency}"]},
            ] 
    sync_queues=  queue.Queue(maxsize=150000) # crossed-thread-sharing queue 
    start_event_for_thread = threading.Event()

    logging.basicConfig(
    filename="./mock-trade/logs/output.log",  # Log file name
    filemode="w",           # Overwrite the file on each run, use "a" for append
    level=logging.INFO,    # Set the log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s"  # Log message format
    )

    # start !
    threads = []
    for i in range(2):
        
        thread = threading.Thread(target=send_websocket_request, args=(exchanges[i], urls[i], msgs[i], sync_queues, start_event_for_thread))
        threads.append(thread)
        thread.start()
    start_event_for_thread.set()

    for thread in threads:
        thread.join()
