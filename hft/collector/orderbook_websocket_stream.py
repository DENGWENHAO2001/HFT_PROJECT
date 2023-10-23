import os
import sys

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.append(ROOT)
sys.path.insert(0, ".")

import json
import websocket
import threading
from queue import Queue
import argparse

from hft.logger import get_logger
from hft.utils import dict_to_args

class OrderBookTimeWebsocketStream(threading.Thread):
    def __init__(self, args, queue, logger = None):
        super(OrderBookTimeWebsocketStream, self).__init__()

        self.args = args
        self.queue = queue

        self.interval = 1
        self.code = self.args.stream.code.lower()
        self.base = self.args.stream.base.lower()

        self.logger = logger

        self.orderbook_time_stream = f"{self.code}{self.base}@depth@1000ms"  # Auxiliary orderbook stream to get timestamp aligned with kline

        self.orderbook_time = None
        self.timestep = None

    def process_orderbook_timestep(self, data):
        timestep = data["E"]
        firstUpdateId = data["U"]
        lastUpdateId = data["u"]

        data = {
            "timestep":timestep,
            "firstUpdateId":firstUpdateId,
            "lastUpdateId":lastUpdateId
        }
        return data

    def on_open(self, ws):
        self.logger.info("WebSocket connection opened")

    def on_error(self, ws, error):
        self.logger.info(f"WebSocket error: {error}")

    def on_close(self, ws):
        self.logger.info("WebSocket connection closed")

    def on_message(self, ws, message):
        data = json.loads(message)

        stream_name = data["stream"]

        if stream_name.startswith(self.orderbook_time_stream):
            self.orderbook_time = self.process_orderbook_timestep(data["data"])
            #self.logger.info("orderbook_timestep" + " # " + json.dumps(self.orderbook_time))
        try:
            if self.orderbook_time:
                self.timestep = self.orderbook_time["timestep"] // 1000

                # self.logger.info("Stored orderbook timestep in OBTime-Queue, timestep: {}, firstUpdateId: {}, lastUpdateId: {}".format(self.timestep,
                #                     self.orderbook_time["firstUpdateId"], self.orderbook_time["lastUpdateId"]))
                self.queue.put(self.orderbook_time)

                self.orderbook_time = None
        except Exception as e:
            print(e)

    def run(self):
        ws = websocket.WebSocketApp(
            f"wss://stream.binance.com:9443/stream?streams={self.orderbook_time_stream}",
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close,
            on_message=self.on_message
        )
        ws.run_forever()


class OrderbookDataWebsocketStream(threading.Thread):
    def __init__(self, args, queue, logger = None):
        super(OrderbookDataWebsocketStream, self).__init__()

        self.args = args
        self.queue = queue

        self.interval = 1
        self.code = self.args.stream.code.lower()
        self.base = self.args.stream.base.lower()

        self.logger = logger

        self.orderbook_stream = f"{self.code}{self.base}@depth5@1000ms"  # 1s, 5 levels

        self.orderbook_data = None

        self.last_update_id = None

    def process_orderbook_data(self, data):
        lastUpdateId = data["lastUpdateId"]
        bid = data["bids"]
        bid1_price, bid1_size = bid[0]
        bid2_price, bid2_size = bid[1]
        bid3_price, bid3_size = bid[2]
        bid4_price, bid4_size = bid[3]
        bid5_price, bid5_size = bid[4]

        ask = data["asks"]
        ask1_price, ask1_size = ask[0]
        ask2_price, ask2_size = ask[1]
        ask3_price, ask3_size = ask[2]
        ask4_price, ask4_size = ask[3]
        ask5_price, ask5_size = ask[4]

        data = {
            "lastUpdateId": lastUpdateId,
            "bid1_price": bid1_price,
            "bid1_size": bid1_size,
            "bid2_price": bid2_price,
            "bid2_size": bid2_size,
            "bid3_price": bid3_price,
            "bid3_size": bid3_size,
            "bid4_price": bid4_price,
            "bid4_size": bid4_size,
            "bid5_price": bid5_price,
            "bid5_size": bid5_size,
            "ask1_price": ask1_price,
            "ask1_size": ask1_size,
            "ask2_price": ask2_price,
            "ask2_size": ask2_size,
            "ask3_price": ask3_price,
            "ask3_size": ask3_size,
            "ask4_price": ask4_price,
            "ask4_size": ask4_size,
            "ask5_price": ask5_price,
            "ask5_size": ask5_size,
        }
        return data

    def on_open(self, ws):
        self.logger.info("WebSocket connection opened")

    def on_error(self, ws, error):
        self.logger.info(f"WebSocket error: {error}")

    def on_close(self, ws):
        self.logger.info("WebSocket connection closed")

    def on_message(self, ws, message):
        data = json.loads(message)

        stream_name = data["stream"]

        if stream_name.startswith(self.orderbook_stream):
            self.orderbook_data = self.process_orderbook_data(data["data"])
            #self.logger.info("orderbook_data" + " # " + json.dumps(self.orderbook_data))

        try:
            if self.orderbook_data:
                self.last_update_id = self.orderbook_data["lastUpdateId"]

                # self.logger.info("Stored orderbook data in OBData-Queue, lastUpdateId: {}".format(self.last_update_id))
                self.queue.put(self.orderbook_data)

                self.orderbook_data = None
                self.last_update_id = None

        except Exception as e:
            print(e)

    def run(self):
        ws = websocket.WebSocketApp(
            f"wss://stream.binance.com:9443/stream?streams={self.orderbook_stream}",
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close,
            on_message=self.on_message
        )
        ws.run_forever()

class OrderBookWebsocketStream(threading.Thread):
    def __init__(self, args, queue, logger = None):
        super(OrderBookWebsocketStream, self).__init__()

        self.args = args
        self.queue = queue

        self.interval = 1
        self.code = self.args.stream.code.lower()
        self.base = self.args.stream.base.lower()

        self.logger = logger

        self.orderbook_data_queue = Queue()
        self.orderbook_time_queue = Queue()

        self.orderbook_stream = OrderbookDataWebsocketStream(args, self.orderbook_data_queue, self.logger).start()
        self.orderbook_time_stream = OrderBookTimeWebsocketStream(args, self.orderbook_time_queue, self.logger).start()

        self.orderbook_data = None
        self.orderbook_time = None

        self.timestep = None
        self.orderbook_time_first_update_id = None
        self.orderbook_time_last_update_id = None
        self.orderbook_data_last_update_id = None

    def run(self):
        while True:
            self.orderbook_data = self.orderbook_data_queue.get()
            self.orderbook_time = self.orderbook_time_queue.get()

            self.timestep = self.orderbook_time["timestep"] // 1000
            self.orderbook_time_first_update_id = self.orderbook_time["firstUpdateId"]
            self.orderbook_time_last_update_id = self.orderbook_time["lastUpdateId"]
            self.orderbook_data_last_update_id = self.orderbook_data["lastUpdateId"]

            while self.orderbook_data_last_update_id < self.orderbook_time_first_update_id:
                self.orderbook_data = self.orderbook_data_queue.get()
                self.orderbook_data_last_update_id = self.orderbook_data["lastUpdateId"]

            while self.orderbook_data_last_update_id > self.orderbook_time_last_update_id:
                self.orderbook_time = self.orderbook_time_queue.get()
                self.timestep = self.orderbook_time["timestep"] // 1000
                self.orderbook_time_first_update_id = self.orderbook_time["firstUpdateId"]
                self.orderbook_time_last_update_id = self.orderbook_time["lastUpdateId"]

            if self.orderbook_data_last_update_id >= self.orderbook_time_first_update_id \
                    and self.orderbook_data_last_update_id <= self.orderbook_time_last_update_id:

                self.orderbook_data["timestep"] = self.orderbook_time["timestep"]
                self.orderbook_data.pop("lastUpdateId")

                self.timestep = self.orderbook_data["timestep"] // 1000

                # self.logger.info("Stored orderbook data in OrderBook-Queue, timestep is {}".format(self.timestep))
                self.queue.put(self.orderbook_data)

                self.orderbook_data = None

if __name__ == '__main__':
    import argparse
    import yaml
    def get_args_parser():
        parser = argparse.ArgumentParser("METD pre-training", add_help=False)
        parser.add_argument("--config", default=os.path.join(ROOT, "configs", "config_x1m.yaml"), type=str)
        args = parser.parse_args()

        assert os.path.exists(args.config), "path of config is not exists!"
        with open(args.config) as f:
            config = yaml.safe_load(f)

        config.update(args._get_kwargs())
        args = dict_to_args(config)
        return args

    args = get_args_parser()

    logger = get_logger(os.path.join(ROOT, "logs", "orderbook"), "orderbook")

    queue = Queue()
    orderbook_wss = OrderBookWebsocketStream(args = args, queue=queue, logger=logger)
    orderbook_wss.start()