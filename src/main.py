import queue
import json
import asyncio 
import websockets
import gzip
import time
import os
import threading
import logging
import pickle
from pathlib import Path
from datetime import datetime, timedelta

class Response() :
    def __init__(self, exchange, ts):
        self.exchange = exchange
        self.ts = ts
        self.data = ''
    
async def ws_handler(id, url, msg, collective_data, start_event):
    print("ready to start")
    start_event.wait()
    #duration = 300  # in secs
    
    print(f"start ! {id}" )
    time.sleep(1)  # stop for main thread to ready
    
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
                    response_ = Response(id, ts)
                    current_time = time.time()  # for ping pong
                    
                    if isinstance(recv, bytes) :
                        # for binary data
                        decompress_data = gzip.decompress(recv).decode("utf-8") # parse to string
                        response_ .data = decompress_data
                        collective_data.put(response_)
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
                        response_.data = recv  # raw msg
                        collective_data.put(response_)  # tuck into queue

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
            logging.info(f"reconnect for {id} err msg is {e}") 
            await asyncio.sleep(1)
        
        except websockets.exceptions.ConnectionClosedOK:
            logging.info("Connection closed gracefully (1001). Retrying...")
            await asyncio.sleep(1)

        except asyncio.TimeoutError as e:
            logging.info(f"Timeout occurred for {id}, err msg is {e} , reconnecting ")
            await asyncio.sleep(1)  
            
                

def send_websocket_request(thread_id, ws_url, message, df, start_event):
    asyncio.run(ws_handler(thread_id, ws_url, message, df, start_event))

def search_data_cnt (directory) :
    file_list =  os.listdir(directory)
    biggest_file = 0  # default 
    
    for f in file_list :
        if int(f[ : f.find('.')]) > biggest_file:
            biggest_file = int(f[ : f.find('.')]) # update biggest file 

    print(f"file of biggest {biggest_file}")
    return biggest_file

def production_thread(target_currency, base_currency="USDT"):
    # main thread
    
    start_event = threading.Event()
    ts = time.time_ns()

    threads = []
    '''
    ids = ["bitget", "htx", "gateio", "bybit"]
    urls = [ "wss://ws.bitget.com/v2/ws/public", 
            "wss://api.hbdm.com/linear-swap-ws",
            "wss://fx-ws.gateio.ws/v4/ws/usdt",
            "wss://stream.bybit.com/v5/public/linear"
            ] 
    msgs = [{"op" :"subscribe", "args" : [ {"instType" : f"{base_currency}-FUTURES", "channel" : "books1", "instId" : f"{target_currency}{base_currency}"}]}, 
            {"sub" :f"market.{target_currency}-{base_currency}.depth.step0", "id" : "test0"},
            {"time" : ts, "channel" : "futures.book_ticker", "event" : "subscribe", "payload" : [f"{target_currency}_{base_currency}"]},
            {"op" :"subscribe", "args" : [f"orderbook.1.{target_currency}{base_currency}"],}
            ] 
    '''
    ids = ["htx", "gateio"]
    urls = [ 
            "wss://api.hbdm.com/linear-swap-ws",
            "wss://fx-ws.gateio.ws/v4/ws/usdt",
            ] 
    msgs = [ 
            {"sub" :f"market.{target_currency}-{base_currency}.depth.step0", "id" : "test0"},
            {"time" : ts, "channel" : "futures.book_ticker", "event" : "subscribe", "payload" : [f"{target_currency}_{base_currency}"]},
            ] 
    sync_queues=  queue.Queue(maxsize=100000) # cross thread sharing queue 
    
    # worker threads to collecting data
    for i in range(len(ids)):
        
        thread = threading.Thread(target=send_websocket_request, args=(ids[i], urls[i], msgs[i], sync_queues, start_event))
        threads.append(thread)
        thread.start()
        
    logging.info("All threads are ready. Starting in 1 seconds...")
    start_time = int(time.time())
    last_write_time = start_time

    current_date = datetime.now().strftime("%Y-%m-%d")
    directory = Path(f"./data/{target_currency}/{current_date}")
    directory.mkdir(parents=True, exist_ok=True)
    current_directory = Path.cwd()
    start_event.set()  # Signal threads to start
    
    data_cnt = search_data_cnt(directory)
    log_cnt = 0
    try :

        while True :
            #time.sleep(30)  # waiting peroid 
            # Write queue data to a binary file
            #if sync_queues.qsize() >= 100000 * 0.8 :
            current_time = time.time()
            if current_time - last_write_time >= 600 :  # write file every ten minutes 
                #end_time = int(time.time())
                if current_time - start_time >= 86400 :  # over one day

                    start_time = time.time()  # update start time
                    current_date = datetime.strptime(current_date, "%Y-%m-%d")
                    current_date =  current_date + timedelta(days=1)
                    current_date = current_date.strftime("%Y-%m-%d")
                    directory = Path(f"./data/{target_currency}/{current_date}")
                    directory.mkdir(parents=True, exist_ok=True)
                    data_cnt = 0  # reset

                    logger = logging.getLogger()  # Get the root logger
                    for handler in logger.handlers[:]:  # Iterate over a copy of the list
                        logger.removeHandler(handler)  # Remove existing handlers

                    # Add a new FileHandler with the new filename
                    log_cnt += 1
                    new_handler = logging.FileHandler(f"./logs/output_{log_cnt}.log", mode="w")
                    new_handler.setLevel(logging.INFO)
                    new_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
                    logger.addHandler(new_handler)
                                
                
                with open(f"{current_directory}/data/{target_currency}/{current_date}/{data_cnt}.bin", "wb") as binary_file:
                    while not sync_queues.empty():
                        data = sync_queues.get()
                        pickle.dump(data, binary_file)  # Serialize and write each item to the file
                    logging.info(f"Written to file: {data_cnt}.bin")
                
                
                data_cnt += 1  # upadte counter 
                last_write_time = current_time  # update write time
                         

    except KeyboardInterrupt:
        print("stop by user")
    except Exception as e :
        print("unexpected error")
        raise e
        

    for thread in threads:
        thread.join()

    print("All threads have finished.")



if __name__ == "__main__":
     # Configure the logging system
    directory = Path(f"./logs/")
    directory.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
    filename="./logs/output_0.log",  # Log file name
    filemode="w",           # Overwrite the file on each run, use "a" for append
    level=logging.INFO,    # Set the log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s"  # Log message format
    )


    targets = [ "XRP", "DOGE"]
    threads = []
    for i in range(len(targets)):
        
        thread = threading.Thread(target=production_thread, args=(targets[i], "USDT"))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
