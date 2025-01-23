import pandas as pd
import numpy as np
import queue
import requests
import json
import asyncio 
import websockets
import nest_asyncio
import gzip
import time
import threading
import logging
import matplotlib.pyplot as plt 
from matplotlib.ticker import MaxNLocator
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
            async with websockets.connect(url) as websocket:
                # Send a message
                await websocket.send(json.dumps(msg))
                logging.info(f"Sent: {msg}")
                
                while True :
                    # Receive a response
                    recv = await websocket.recv()
                    ts = time.time_ns()  # universial local ts
                    response_ = Response(id, ts)
                    #logging.info(recv)
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
                            print(f"pong : {pong} " )     
                            # Create and send the pong response
                            pong_message = {"pong": int(pong)}
                            await websocket.send(json.dumps(pong_message))

                        '''
                        # try :
                            
                        #     response["local_ts"] = ts
                        #     new_data =  {"bids" : float(response["tick"]["bids"][0][0]), "asks" : float(response["tick"]["asks"][0][0])}  # !!!
                        #     response["exchange"] = "htx"
                            
                        #     collective_data.put(response_)
                        #     #print(response)
                        #     #collective_data.loc[ts] = {"bids" : float(response["tick"]["bids"][0][0]), "asks" : float(response["tick"]["asks"][0][0])}

                        # except websockets.exceptions.ConnectionClosedError as e:
                        #     print(f"Connection failed to connect: {e}")   
                        #     raise e 
                            
                            
                        # except Exception as e :
                        #     print(f"error {e} at htx")
                        #     print(f"errpr msg : {decompress_data}")
                            
                        
                        #     if "ping" in response.keys():
                        #         # Extract the ping timestamp
                        #         ping_timestamp = response["ping"]
                        #         pong = decompress_data[(decompress_data.find("ping")+6) :  decompress_data.find(',')]
                        #         print("pong " ,  pong )     
                        #         # Create and send the pong response
                        #         pong_message = {"pong": int(pong)}
                        #         await websocket.send(json.dumps(pong_message))
                        #         #print(f"Sent pong msg: {pong_message}")
                        '''  

                    else :
                        # other exchanges
                        
                        response_.data = recv
                        collective_data.put(response_)

                    if current_time - start_time >= 540 :   # 9 mins
                       # ping-pong
                        if id == "gateio" :
                            ping_msg = {"time" : f"{int(current_time)}", "channel" : "futures.ping"}
                            await websocket.send(json.dumps(ping_msg))
                            logging.info(f"Sent: {ping_msg}")

                        elif id == "bybit" : 
                            ping_msg = { "op": "ping"}
                            await websocket.send(json.dumps(ping_msg))
                            logging.info(f"Sent: {ping_msg}")


                        start_time = current_time  # update time
                        
                        '''
                        try :
                            #ts = time.time_ns()
                            response["local_ts"] = ts
                            if id == "gateio" :
                                 
                                    response_ = Response("gateio", ts, response)
                                    new_data =  {"bids" : float(response["result"]["b"]), "asks" : float(response["result"]["a"])}
                                    response["exchange"] = "gateio"
                                    collective_data.put(response)
                                    #print(response)
                                    #collective_data.loc[ts] = {"bids" : float(response["result"]["b"]), "asks" : float(response["result"]["a"])}
                                
                            elif id == "bybit":
                                
                                
                                try :
                                
                                    response["exchange"] = "bybit"
                                    response["data"]['a'][0][0] =  float(response["data"]['a'][0][0])
                                    response["data"]['b'][0][0] =  float(response["data"]['b'][0][0])
                                    #collective_data.loc[ts] = {"bids" : float(best_bid), "asks" : float(best_ask)}
                                    #new_data = {"bids" : best_bid, "asks" : best_ask}
                                    collective_data.put(response_)
                                    #print(response)
                                    response_ = Response("bybit", ts, response)
                                except IndexError as e :
                                    # bids or asks is missing
                                    print(f"error {e} at bybit")
                                    print(f"error msg : {response}")
                                    if response["data"]['b'] != []:
                                        response["data"]['b'][0][0] =  float(response["data"]['b'][0][0])
                                    if  response["data"]['a'] != []:
                                        response["data"]['a'][0][0] =  float(response["data"]['a'][0][0])
                                    
                                    # new_data = {"bids" : float(best_bid), "asks" : float(best_ask)}
                                    response["exchange"] = "bybit"
                                    response_ = Response("bybit", ts, response)
                                    collective_data.put(response_)
                                    #print(response)
                                    #collective_data.loc[ts] = {"bids" : float(best_bid), "asks" : float(best_ask)}
                                    
                            elif id == "bitget":
                                
                                response["data"][0]['bids'][0][0] = float(response["data"][0]['bids'][0][0])
                                response["data"][0]['asks'][0][0] = float(response["data"][0]['asks'][0][0])
                                #new_data =  {"bids" : float(response["data"][0]['bids'][0][0]), "asks" :  float(response["data"][0]['asks'][0][0])}
                                response["exchange"] = "bitget"
                                response_ = Response("bitget", ts, response)
                                collective_data.put(response_)
                                #print(response)
                                #collective_data.loc[ts] = {"bids" : float(response["data"][0]['bids'][0][0]), "asks" :  float(response["data"][0]['asks'][0][0])}

                        except websockets.exceptions.ConnectionClosedError as e:
                            print(f"Connection failed to start: {e}")   
                            raise e 

                        except Exception as e :
                            print(f"error {e} at {id}")
                            print(f"error msg : {response}")
                        '''
               
                                    
        except websockets.exceptions.ConnectionClosedError as e:
            logging.info(f"reconnect for {id}") 

        except asyncio.TimeoutError:
            print(f"Timeout occurred for {id}. Retrying...")
            await asyncio.sleep(5)  
            
                
    return

def send_websocket_request(thread_id, ws_url, message, df, start_event):
    asyncio.run(ws_handler(thread_id, ws_url, message, df, start_event))



def production_thread(target_currency, base_currency="USDT"):
    # main thread
    
    start_event = threading.Event()
    ts = time.time_ns()
    threads = []
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
    sync_queues=  queue.Queue(maxsize=100000) # cross thread sharing queue 
    
    # worker threads to collecting data
    for i in range(4):
        
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
    
    # main thread to collecting data
    
    data_cnt = 0
    log_cnt = 0
    # duration = 10
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


    targets = ["ETH", "BTC", "XRP", "DOGE"]
    threads = []
    for i in range(4):
        
        thread = threading.Thread(target=production_thread, args=(targets[i], "USDT"))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
