# Licensed under GPLv3. Maintainer: Lukas Kaupp <lkaupp>

import csv
import queue
import threading
from datetime import datetime, time, timedelta
from pathlib import Path
from time import mktime

import websocket
import json
import ssl
from ruamel.yaml import YAML


def subscribe_to_channel(ws, channel):
    params = {
        'event': 'bts:subscribe',
        'data': {
            'channel': 'live_trades_' + channel
        }
    }
    params_json = json.dumps(params)
    ws.send(params_json)


def on_open(ws, config):
    print('web-socket connected.')

    print('register to:')
    _, channels = generate_paths_n_channels(config['markets']['bitstamp']['coins'])
    for channel in channels:
        print('-' + channel)
        subscribe_to_channel(ws, channel)

    print('all registered')


def on_message(ws,  q, reconnect_event, data):
    data = json.loads(data)
    if 'event' in data:
        if data['event'] == 'trade':
            q.put(data)
        elif data['event'] == 'bts:request_reconnect':
            reconnect_event.is_set()



def on_error(ws, reconnect_event, msg):
    print(msg)
    reconnect_event.is_set()

def write_header_or_append_line(handle, writer, line, print_long_price):
    if handle.tell() == 0:
        writer.writerow(['timestamp ms', 'type', 'amount', 'price'])
    row = list(map(lambda x: line['data'][x], ['microtimestamp', 'type']))
    row.append(f'{line["data"]["amount"]:.8f}')

    if print_long_price:
        row.append(f'{line["data"]["price"]:.8f}')
    else:
        row.append(f'{line["data"]["price"]:.2f}')

    writer.writerow(row)


def generate_paths_n_channels(coins):
    paths = []
    channels = []

    for key, values in coins.items():
        for value in values:
            paths.append(Path(str(key).upper() + '_' + str(value).upper()))
            channels.append(str(key)+str(value))

    return paths, channels



def csv_writer(event, q, config):
    paths, channels = generate_paths_n_channels(config['markets']['bitstamp']['coins'])

    for path in paths:
        path.mkdir(parents=True, exist_ok=True)

    filename = datetime.now().strftime('%d_%m_%Y.csv')
    filehdls = [open(path / filename,'a',newline='', encoding='utf-8') for path in paths]
    transformed_files = {channel: [filehdl, csv.writer(filehdl), 0] for path, channel, filehdl in zip(paths, channels, filehdls)}

    while True:

        message = q.get()
        msg_index = str(message['channel']).rfind('_') + 1

        if msg_index > 1:
            channel_name = message['channel'][msg_index:]
            transform_array = transformed_files[channel_name]

            in_eth = channel_name.rfind('eth') > 0
            in_btc = channel_name.rfind('btc') > 0

            if in_eth or in_btc:
                write_header_or_append_line(transform_array[0], transform_array[1], message, True)
            else:
                write_header_or_append_line(transform_array[0], transform_array[1], message, False)
                
            transformed_files[channel_name][2] = transform_array[2] + 1

            if(transformed_files[channel_name][2] == 100):
                transformed_files[channel_name][2] = 0
                transform_array[0].flush()

        if event.is_set() and q.empty():
            break

    for key in transformed_files:
        transformed_files[key][0].close()



def websocket_watcher(reconnect_event, close_event, q, config):
    marketdata_ws = websocket.WebSocketApp(config['markets']['bitstamp']['ws_endpoint'], on_open=lambda ws: on_open(ws, config), on_message=lambda ws, data: on_message(ws, q, reconnect_event, data),
                                           on_error=lambda ws: on_error(ws, reconnect_event))
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
    yaml = YAML(typ='safe')
    with open('scripts/collector.yaml') as config_hdl:
        config = yaml.load(config_hdl)

    # Start Websocket Data Retrieval
    # Create a Threading Event to signal shutdown between threads
    final_event = threading.Event()
    q = queue.Queue(maxsize=0)
    reconnect_event = threading.Event()

    # Create a WSWatcher thread that restart the WS connection if error appeared or we get asked for a reconnect
    wst = threading.Thread(target=websocket_watcher, kwargs={'reconnect_event': reconnect_event, 'close_event': final_event, 'q': q, 'config': config}, daemon=True)
    wst.start()

    # Event to signal stop between Threads
    event = threading.Event()

    # Create CSVWriter, turn WSMessages to CSV
    writer = threading.Thread(target=csv_writer, kwargs={'event': event, 'q': q, 'config': config})
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









