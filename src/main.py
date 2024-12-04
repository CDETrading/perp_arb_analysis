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
import matplotlib.pyplot as plt 
from matplotlib.ticker import MaxNLocator
import pickle

async def ws_handler(id, url, msg, collective_data, start_event):
    print("ready to start")
    start_event.wait()
    duration = 10  # in secs
    print("start !")
    time.sleep(1)  # stop for main thread to ready
    
    start_time = time.time()
    while True:
        try :
            async with websockets.connect(url) as websocket:
                # Send a message
                await websocket.send(json.dumps(msg))
                print(f"Sent: {msg}")
                

                while time.time() - start_time <= duration :
                    # Receive a response
                    recv = await websocket.recv()
                    
                    if isinstance(recv, bytes) :
                        # for binary data
                        decompress_data = gzip.decompress(recv)
                        response = json.loads(decompress_data.decode("utf-8"))
                        
                        try :
                            ts = time.time_ns()
                            response["local_ts"] = ts
                            new_data =  {"bids" : float(response["tick"]["bids"][0][0]), "asks" : float(response["tick"]["asks"][0][0])}
                            collective_data.put(response)
                            #collective_data.loc[ts] = {"bids" : float(response["tick"]["bids"][0][0]), "asks" : float(response["tick"]["asks"][0][0])}

                        except websockets.exceptions.ConnectionClosedError as e:
                            print(f"Connection failed to connect: {e}")   
                            raise e 
                            
                            
                        except Exception as e :
                            print(f"error {e} at htx")
                        
                            if "ping" in response.keys():
                                # Extract the ping timestamp
                                ping_timestamp = response["ping"]
                                
                                # Create and send the pong response
                                pong_message = {"pong": ping_timestamp}
                                await websocket.send(json.dumps(pong_message))
                                print(f"Sent pong msg: {pong_message}")
                            

                    else :
                        # other exchanges
                        response = json.loads(recv)    
                        # print(f"Received at {id}")
                        # print(response) 
                        
                        try :
                            ts = time.time_ns()
                            response["local_ts"] = ts
                            if id == "gateio" :
                                new_data =  {"bids" : float(response["result"]["b"]), "asks" : float(response["result"]["a"])}
                                collective_data.put(response)
                                #collective_data.loc[ts] = {"bids" : float(response["result"]["b"]), "asks" : float(response["result"]["a"])}
                            elif id == "bybit":
                                
                                
                                try :
                                    response["data"]['a'][0][0] =  float(response["data"]['a'][0][0])
                                    response["data"]['b'][0][0] =  float(response["data"]['b'][0][0])
                                    #collective_data.loc[ts] = {"bids" : float(best_bid), "asks" : float(best_ask)}
                                    #new_data = {"bids" : best_bid, "asks" : best_ask}
                                    collective_data.put(response)
                                except IndexError as e :
                                    if response["data"]['b'] != []:
                                        response["data"]['b'][0][0] =  float(response["data"]['b'][0][0])
                                    if  response["data"]['a'] != []:
                                        response["data"]['a'][0][0] =  float(response["data"]['a'][0][0])
                                    
                                    # new_data = {"bids" : float(best_bid), "asks" : float(best_ask)}
                                    collective_data.put(response)
                                    #collective_data.loc[ts] = {"bids" : float(best_bid), "asks" : float(best_ask)}
                                    
                            elif id == "bitget":
                                response["data"][0]['bids'][0][0] = float(response["data"][0]['bids'][0][0])
                                response["data"][0]['asks'][0][0] = float(response["data"][0]['asks'][0][0])
                                #new_data =  {"bids" : float(response["data"][0]['bids'][0][0]), "asks" :  float(response["data"][0]['asks'][0][0])}
                                collective_data.put(response)
                                #collective_data.loc[ts] = {"bids" : float(response["data"][0]['bids'][0][0]), "asks" :  float(response["data"][0]['asks'][0][0])}

                        except websockets.exceptions.ConnectionClosedError as e:
                            print(f"Connection failed to start: {e}")   
                            raise e 

                        except Exception as e :
                            print(f"error {e} at {id} and pass ")
                                    
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"reconnect for {id}")   
            
                
    print(f"{id} finish")
    return

def send_websocket_request(thread_id, ws_url, message, df, start_event):
    asyncio.run(ws_handler(thread_id, ws_url, message, df, start_event))


if __name__ == "__main__":

    # main thread
    # threading
    #nest_asyncio.apply()

    start_event = threading.Event()
    ts = time.time_ns()
    target_currency = "ETH"
    base_currency = "USDT"
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
    dfs = [pd.DataFrame(columns=["local_ts", "bids", "asks"]) for _ in range(4)] # data storage 
    sync_queues=  queue.Queue(maxsize=10000) # cross thread sharing queue 
    # worker threads to collecting data
    for i in range(4):
        
        thread = threading.Thread(target=send_websocket_request, args=(ids[i], urls[i], msgs[i], sync_queues, start_event))
        threads.append(thread)
        thread.start()
        
    print("All threads are ready. Starting in 1 seconds...")
    time.sleep(1)  # Optional delay before starting
    start_event.set()  # Signal threads to start

    # # main thread to collecting data
    # prod_data_queues = [queue.Queue() for _ in range(4)]  # to store data from worker thread
    # cache_data = [ 0 for _ in range(4) ]  # to cache data for each exchange 
    start_time = int(time.time())
    # duration = 10
    try :

        while True :
            time.sleep(600)  # collecting peroid 
            # Write queue data to a binary file
            end_time = int(time.time())
            with open(f"/home/timchen/perp_arb_analysis/data/{target_currency}_data_from_{start_time}_to_{end_time}.bin", "wb") as binary_file:
                while not sync_queues.empty():
                    data = sync_queues.get()
                    pickle.dump(data, binary_file)  # Serialize and write each item to the file
                    print(f"Written to file: {data}")
            start_time = end_time
                    
            

    except KeyboardInterrupt:
        print("stop by user")
    except Exception as e :
        print("unexpected error")
        raise e
        

    for thread in threads:
        thread.join()

    print("All threads have finished.")