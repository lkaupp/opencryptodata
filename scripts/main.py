# Licensed under GPLv3. Maintainer: Lukas Kaupp <lkaupp>

import csv
import queue
import threading
from datetime import datetime, time, timedelta
from time import mktime

import websocket
import json
import ssl

bitstamp_endpoint = 'wss://ws.bitstamp.net'
q = queue.Queue(maxsize=0)
reconnect_event = threading.Event()
def subscribe_to_channel(ws, channel):
    params = {
        'event': 'bts:subscribe',
        'data': {
            'channel': 'live_trades_' + channel
        }
    }
    params_json = json.dumps(params)
    ws.send(params_json)

def subscribe_marketdata(ws):
    subscribe_to_channel(ws,'btcusd')
    subscribe_to_channel(ws,'btceur')

def on_open(ws):
    print('web-socket connected.')
    subscribe_marketdata(ws)


def on_message(ws, data):
    global q
    global reconnect_event
    data = json.loads(data)
    if 'event' in data:
        if data['event'] == 'trade':
            q.put(data)
        elif data['event'] == 'bts:request_reconnect':
            reconnect_event.is_set()



def on_error(ws, msg):
    print(msg)
    global reconnect_event
    reconnect_event.is_set()

def write_header_or_append_line(handle, writer, line):
    if handle.tell() == 0:
        writer.writerow(['timestamp ms', 'type', 'amount', 'price'])
    row = list(map(lambda x: line['data'][x], ['microtimestamp', 'type']))
    row.append(f'{line["data"]["amount"]:.8f}')
    row.append(f'{line["data"]["price"]:.2f}')
    writer.writerow(row)

def csv_writer(event):
    global q
    filename = datetime.now().strftime('%d_%m_%Y.csv')
    with open("BITCOIN_USD/"+filename, 'a',newline='', encoding='utf-8') as btcusd,\
        open("BITCOIN_EUR/"+filename, 'a',newline='', encoding='utf-8') as btceur:

        csv_btcusd = csv.writer(btcusd)
        csv_btceur = csv.writer(btceur)
        counter = 0
        while True:

            message = q.get()

            counter = counter+1

            if 'btceur' in message['channel']:
                write_header_or_append_line(btceur, csv_btceur, message)
            elif 'btcusd' in message['channel']:
                write_header_or_append_line(btcusd, csv_btcusd, message)

            if counter == 100:
                btcusd.flush()
                btceur.flush()
                counter = 0

            if event.is_set() and q.empty():

                break

        btceur.close()
        btcusd.close()



def websocket_watcher(reconnect_event, close_event):
    marketdata_ws = websocket.WebSocketApp(bitstamp_endpoint, on_open=on_open, on_message=on_message,
                                           on_error=on_error)
    wst = threading.Thread(target=marketdata_ws.run_forever, kwargs={'sslopt': {'cert_reqs': ssl.CERT_NONE}})
    wst.start()
    while True:


        #asked for reconnect or error appeared  = restart WS by close socket and restart thread
        if reconnect_event.is_set():

            reconnect_event.clear()
            marketdata_ws.close()
            wst.join()

            wst = threading.Thread(target=marketdata_ws.run_forever, kwargs={'sslopt': {'cert_reqs': ssl.CERT_NONE}})
            wst.start()

        #reconnect if connection gets lost
        if not wst.is_alive():
            marketdata_ws.close()
            wst.join()

            wst = threading.Thread(target=marketdata_ws.run_forever, kwargs={'sslopt': {'cert_reqs': ssl.CERT_NONE}})
            wst.start()

        if close_event.is_set():
            marketdata_ws.close()
            wst.join()
            break



def kill_after_a_day(event, next_day_midnight,reconnect_event):
    while True:
        dt = datetime.now()
        sec_since_epoch = mktime(dt.timetuple()) + dt.microsecond / 1000000.0
        now = sec_since_epoch * 1000
        if next_day_midnight < now:
            event.set()
            break
        #or by key
        selection = input("Q: Quit")
        if selection == "Q" or selection == "q":
            print("Quitting")
            event.set()
            break
        if selection == "s":
            print("simulate reconnect")
            reconnect_event.set()




# Collect cryptocurrency information from the following markets:
# - bitstamp
# Script is designed to run within a 24 hour cronjob
# 0 0 * * * cd /opencryptodata/ && bash ./scripts/update.sh && python3 ./scripts/main.py

if __name__ == "__main__":
    # Start Websocket Data Retrieval
    # Create a Threading Event to signal shutdown between threads
    final_event = threading.Event()

    # Create a WSWatcher thread that restart the WS connection if error appeared or we get asked for a reconnect
    wst = threading.Thread(target=websocket_watcher, kwargs={'reconnect_event': reconnect_event, 'close_event': final_event}, daemon=True)
    wst.start()

    # Event to signal stop between Threads
    event = threading.Event()

    # Create CSVWriter, turn WSMessages to CSV
    writer = threading.Thread(target=csv_writer, kwargs={'event': event})
    writer.start()

    # Starting at midnight and add 24 hours for the next midnight (date the script shutdowns all threads) and convert the end date to a unix timestamp
    dt = datetime.combine(datetime.today().date(), time.min) + timedelta(hours=24)
    sec_since_epoch = mktime(dt.timetuple()) + dt.microsecond / 1000000.0
    next_day_midnight = sec_since_epoch * 1000

    # Thread that signals the writer to finish and shut down
    killer = threading.Thread(target=kill_after_a_day, kwargs={'event': event, 'next_day_midnight': next_day_midnight, 'reconnect_event': reconnect_event})
    killer.start()
    killer.join()
    writer.join()
    #All threads are gracefully closed. Close the WS and its watcher thread
    final_event.set()









