import os
import sys
ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.append(ROOT)
sys.path.insert(0, ".")

import json
import websocket
import threading
import queue

from hft.logger import get_logger
from hft.utils import dict_to_args

class KlineWebsocketStream(threading.Thread):
    def __init__(self, args, interval, queue, logger = None):
        super(KlineWebsocketStream, self).__init__()

        self.args = args

        self.interval = interval
        self.code = self.args.stream.code.lower()
        self.base = self.args.stream.base.lower()

        self.queue = queue

        self.logger = logger

        self.kline_stream = f"{self.code}{self.base}@kline_{interval}"
        self.kline_data = None

        self.timestep = None

    def process_kline_data(self, data):
        timestep = data["E"]
        kdata = data["k"]
        open = kdata["o"]
        high = kdata["h"]
        low = kdata["l"]
        close = kdata["c"]

        data = {
            "timestep": timestep,
            "open": open,
            "high": high,
            "low": low,
            "close": close
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

        if stream_name.startswith(self.kline_stream):
            self.kline_data = self.process_kline_data(data["data"])

        try:
            self.timestep = self.kline_data["timestep"] // 1000

            # self.logger.info("Stored {} kline data in Kline-Queue, timestep: {}".format(self.interval, self.timestep))
            # self.logger.info(self.kline_data)

            self.queue.put(self.kline_data)

            self.kline_data = None

        except Exception as e:
            print(e)

    def run(self):
        ws = websocket.WebSocketApp(
            f"wss://stream.binance.com:9443/stream?streams={self.kline_stream}",
            on_open=self.on_open,
            on_error=self.on_error,
            on_close=self.on_close,
            on_message=self.on_message
        )
        ws.run_forever()

if __name__ == '__main__':
    import argparse
    import yaml
    def get_args_parser():
        parser = argparse.ArgumentParser("METD pre-training", add_help=False)
        parser.add_argument("--config", default=os.path.join(ROOT, "configs", "config.yaml"), type=str)
        args = parser.parse_args()

        assert os.path.exists(args.config), "path of config is not exists!"
        with open(args.config) as f:
            config = yaml.safe_load(f)

        config.update(args._get_kwargs())
        args = dict_to_args(config)
        return args

    args = get_args_parser()

    logger = get_logger(os.path.join(ROOT, "logs", "kline"), "kline")

    queue = queue.Queue()
    kline_wss = KlineWebsocketStream(args = args, interval='1m', queue=queue, logger=logger)
    kline_wss.run()