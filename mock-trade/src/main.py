import time
import json
import math
import queue
import copy
import gzip
import numpy as np
import pandas as pd
import logging
import threading
import websockets
import asyncio 
from collections import deque

_PHASE = 0  # switch for collecting real-time WS ticker data



async def ws_handler(exchange_id, url, msg, collective_data, quote_queue, start_event):
    print(f"{exchange_id} thread is ready to start")
    trade_cnt = 0
    start_event.wait()  # hold for start singal
    
    print(f"start ! {exchange_id}" )
    start_time = time.time_ns()

    while True:
        try :
            async with websockets.connect(url, ping_interval=10) as websocket:
                # Send a message
                await websocket.send(json.dumps(msg))
                logging.info(f"Sent: {msg}")
                
                while True :

                  
                    # Receive a response
                    recv = await websocket.recv()
                    current_time = time.time_ns() 

                    if trade_cnt == 0 :
                        # filter out subscribed response
                        trade_cnt += 1
                        continue
                   
                    if isinstance(recv, bytes) :
                        # for binary data
                        try : 
                            decompress_data = gzip.decompress(recv).decode("utf-8") # parse to string
                           
                            if decompress_data.find("ping") > 0:
                                # Create and send the pong response
                                pong = decompress_data[(decompress_data.find("ping")+6) :  decompress_data.find(',')]
                                #logging.info(f"pong : {pong} for {exchange_id} " )     
                                pong_message = {"pong": int(pong)}
                                await websocket.send(json.dumps(pong_message))
                        
                            else :
                                curr_data = {'value' :decompress_data, 'ts' :current_time, 'exchange' : exchange_id}
                                if _PHASE == 1 :
                                    collective_data.pop()  # pop
                                    if quote_queue.qsize() > 0  :
                                        quote_queue.get()  # pop
                                    quote_queue.put(curr_data)  # push 
                    
                                collective_data.appendleft(curr_data)  # push 
                               
                        except KeyError as e :
                            print(f"keyerror happen {e} for {exchange_id}")
                            continue
                            
                    else :
                        # non-binary data
                        try :
                            curr_data = {'value' :recv, 'ts' :current_time, 'exchange' : exchange_id} 
                            if _PHASE == 1 :
                                collective_data.pop()  # push
                                if quote_queue.qsize() > 0  :
                                    quote_queue.get()  # pop
                                quote_queue.put(curr_data)  # push 

                            collective_data.appendleft(curr_data)

                        except KeyError as e :
                            print(f"keyerror happen {e} doe {exchange_id}")
                            continue

                    if current_time - start_time >= 60000000000 :   # 1 mins sending period
                       # ping-pong
                        if id == "gateio" :
                            ping_msg = {"time" : f"{int(current_time)}", "channel" : "futures.ping"}
                            #logging.info(f"Sent: {ping_msg} for {exchange_id}")
                            await websocket.send(json.dumps(ping_msg))
                            
                        elif id == "bybit" : 
                            ping_msg = { "op": "ping"}
                            #logging.info(f"Sent: {ping_msg} for {exchange_id}")
                            await websocket.send(json.dumps(ping_msg))

                        start_time = current_time  # update ping-pong start time
                        
                    
                                    
        except websockets.exceptions.ConnectionClosedError as e:
            logging.info(f"reconnect for {exchange_id} err msg is {e}") 
            await asyncio.sleep(1)
        
        except websockets.exceptions.ConnectionClosedOK as e :
            logging.info(f"Connection closed gracefully (1001) for {exchange_id} err msg is {e}")
            await asyncio.sleep(1)

        except asyncio.TimeoutError as e:
            logging.info(f"Timeout occurred for {exchange_id}, err msg is {e} err msg is {e}")
            await asyncio.sleep(1)  
            
            
def send_websocket_request(exchange_id, ws_url, message, deque, queue, start_event):
    asyncio.run(ws_handler(exchange_id, ws_url, message, deque, queue, start_event))

def parse_market_data(market_data_deque) :
    market_data_list = list(market_data_deque)  # copy to list 
    #time_stamp = [] 
    data_htx = []
    data_gateio = []
    for i in range(len(market_data_list)) :
        try : 
            current_data_point = json.loads(market_data_list[i]['value'])
            #time_stamp.append(market_data_list[i]['ts'])

            if market_data_list[i]['exchange'] == 'htx' :
                parsed_data_point = [(current_data_point['tick']['asks'][0][0], current_data_point['tick']['asks'][0][1]),
                                     (current_data_point['tick']['bids'][0][0], current_data_point['tick']['bids'][0][1])]
                data_htx.append(parsed_data_point)
                if len(data_gateio) == 0 :
                    data_gateio.append(float('nan'))
                    continue
                data_gateio.append(data_gateio[-1])
               
            elif market_data_list[i]['exchange'] == 'gateio' :
                parsed_data_point = [(float(current_data_point['result']['a']), float(current_data_point['result']['A'])),
                                       (float(current_data_point['result']['b']), float(current_data_point['result']['B']))]
                
                data_gateio.append(parsed_data_point)
                if len(data_htx) == 0 :
                    data_htx.append(float('nan'))
                    continue
                data_htx.append(data_htx[-1])
            
           
        except KeyError as e:
            logging.info(f"Keyerror happen at {market_data_list[i]['exchange']} , {e}")


    exchange_data_list = [data_htx, data_gateio]
    exchange_data_list = clean_market_data(exchange_data_list)
   
    return calcu_signal(exchange_data_list)
   

def parse_single_mareket_data(market_data) :
    try : 
        current_data_point = json.loads(market_data['value'])
        #time_stamp.append(market_data_list[i]['ts'])
        parsed_data_point = []
        if market_data['exchange'] == 'htx' :
            parsed_data_point = [(current_data_point['tick']['asks'][0][0], current_data_point['tick']['asks'][0][1]),
                                    (current_data_point['tick']['bids'][0][0], current_data_point['tick']['bids'][0][1])]
            
            
        elif market_data['exchange'] == 'gateio' :
            parsed_data_point = [(float(current_data_point['result']['a']), float(current_data_point['result']['A'])),
                                    (float(current_data_point['result']['b']), float(current_data_point['result']['B']))]
                
        return parsed_data_point        
            
           
    except KeyError as e:
        logging.info(f"Keyerror happen at {market_data['exchange']} , {e}")
        
        return -1

def clean_market_data(market_datas) :
    max_start = 0
    
    for data in (market_datas) :
        curr_start = 0 
        for i in range(len(data)) :
            
            if type(data[i]) != float:
                curr_start = i
                break
        if curr_start > max_start :
            max_start = curr_start

    for index, data in enumerate(market_datas)  :
        data = data[max_start : ]
        market_datas[index] = data

    return market_datas
    
def calcu_signal(market_data_list) :
   
    market_data_list[0] = np.array(market_data_list[0])
    market_data_list[1] = np.array(market_data_list[1])

    ask_quote = market_data_list[0][:, 0, 0]
    bid_quote = market_data_list[1][:, 1, 0]

    spread = ask_quote / bid_quote
    mean = np.mean(spread)
    std = np.std(spread)
   
    logging.info(f"current mean : {mean} and std : {std}")

    return mean + std * 3 , mean

    

if __name__ == "__main__" :
    
    record = {}  # to record detailed trading record
    target_currency = "XRP"
    base_currency = "USDT"
    exchanges = ["htx", "gateio"]
    urls = [  
            "wss://api.hbdm.com/linear-swap-ws",
            "wss://fx-ws.gateio.ws/v4/ws/usdt",
            ] 
    ts = time.time_ns()  # for subscribe
    msgs = [
            {"sub" :f"market.{target_currency}-{base_currency}.depth.step0", "id" : "test0"},
            {"time" : ts, "channel" : "futures.book_ticker", "event" : "subscribe", "payload" : [f"{target_currency}_{base_currency}"]},
            ] 
    data_deque = deque(maxlen=150000)  # using deque
    quote_queue = [queue.Queue(maxsize=5), queue.Queue(maxsize=5)]
    lock = threading.Lock()
    start_event_for_thread = threading.Event()

    logging.basicConfig(
        filename="./mock-trade/logs/output.log",  # Log file name
        filemode="w",           # Overwrite the file on each run, use "a" for append
        level=logging.INFO,    # Set the log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format="%(asctime)s - %(levelname)s - %(message)s"  # Log message format
    )

    # start !
    threads = []  # list for WS worker thread
    total_profit = 0  # record ToT profit for testing entire period 
    

    for i in range(2):
        
        thread = threading.Thread(target=send_websocket_request, args=(exchanges[i], urls[i], msgs[i], data_deque, quote_queue[i], start_event_for_thread))
        threads.append(thread)
        thread.start()

    start_event_for_thread.set()  # start to listen to WS
    start_time = time.time_ns() # trading start sign
    while True:
        # receive data cycle
        #print(f" data size : {len(data_deque)}")
       
       
        if time.time_ns() - start_time >= 3600000000000 :
            _PHASE = 1
           
            # ====== main overhead happen ======= # 
            with lock :
                # copy market data
                market_data = copy.deepcopy(data_deque)  # copy from dequeue
                
            # parse market data
            ask = 0
            bid = 0
            order_ask = 0
            order_bid = 0
            orders_existed = False
            converged = False
            curr_spread = 0

            print('start to trade')
            threshold, mean = parse_market_data(market_data)
            print(f'threshold is : {threshold}')

            # ====== main overhead happen ======= # 

            trade_start_time = time.time_ns()
            while True :
                # trade-cycle                
                if quote_queue[0].qsize() > 0  :
                    # htx ask-bid 
                    ask = parse_single_mareket_data(quote_queue[0].get())[0][0]
                else :
                    continue
                if quote_queue[1].qsize() > 0  :
                    # gateio ask-bid
                    bid = parse_single_mareket_data(quote_queue[0].get())[1][0]
                else :
                    continue

                if not converged :
                    curr_spread = ask / bid  # current spread 
                    if not orders_existed :
                    
                        if curr_spread >= threshold:
                            # short-long
                            # placing orders
                            # latency trade 
                            orders_existed = True
                            print("ordered !")
                            time.sleep(0.00004)  # 40 - 50 micro-secs (consider above overhead)
                    else :
                        # pending orders
                        if order_ask == 0 :
                            # making order (consider latency)
                            order_ask = ask
                            order_bid = bid
                        else :
                            # check spread
                            if curr_spread <= mean :
                                print(f"converge !")
                                converged = True
                                time.sleep(0.00004)  # 40 - 50 micro-secs (consider above overhead)
                else :
                    short_result = ((order_ask - ask) / order_ask) * 100
                    long_result = ((bid - order_bid) / order_bid) * 100 
                    curr_result = short_result+long_result
                    total_profit += short_result+long_result
                    print(f"result : short : {short_result}% | long : {long_result}% => total : {(short_result+long_result)}%")
                    logging.info(f"result : short : {short_result}% | long : {long_result}% => total : {curr_result}%")
                    order_ask = 0
                    order_bid = 0
                    orders_existed = False
                    converged = False


                if time.time_ns() - trade_start_time >= 1200000000000 :
                    print("current trade window close")
                    if orders_existed and order_ask > 0 :
                        # not converge during curent pending orders
                        print(f"not converge during curent pending orders => orders cancel")
                        logging.info(f"not converge during curent pending orders => orders cancel")
                        short_result = ((order_ask - ask) / order_ask) * 100
                        long_result = ((bid - order_bid) / order_bid) * 100 
                        curr_result = short_result+long_result
                        total_profit += short_result+long_result
                        print(f"forced result : short : {short_result}% | long : {long_result}% => total : {curr_result}%")
                        logging.info(f"forced result : short : {short_result}% | long : {long_result}% => total : {curr_result}%")

                    break

            print("next trading window")
            if time.time_ns() - start_time >= 1800000000000 :
                print("end 0.5 hr testing period")
                logging.info(f"total proft for 0.5 hr : {total_profit}%")
                break
               
          

    for thread in threads:
        thread.join()
        logging.info("All threads joined")
        print("Testing finished")

