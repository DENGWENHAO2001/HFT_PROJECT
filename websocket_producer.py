import json
import queue
import os
import sys
import threading
import argparse
import yaml
import time

ROOT = os.path.dirname(os.path.abspath(__file__))#os.path.dirname(os.path.dirname(__file__))
sys.path.append(ROOT)
sys.path.insert(0, "hft/tools")

from celery_app import app
from hft.logger import get_logger
from hft.rabbitmq import pika_connection
from hft.collector import OrderBookWebsocketStream, KlineWebsocketStream
from hft.utils import dict_to_args

def get_args_parser():
    parser = argparse.ArgumentParser("HFT real-time testing", add_help=False)
    parser.add_argument("--config", default=os.path.join(ROOT, "hft/configs", "config.yaml"), type=str)
    args = parser.parse_args()

    assert os.path.exists(args.config), "path of config is not exists!"
    with open(args.config) as f:
        config = yaml.safe_load(f)

    config.update(args._get_kwargs())
    args = dict_to_args(config)
    return args

class Producer(threading.Thread):
    def __init__(self, args):
        super(Producer, self).__init__()
        self.args = args

        self.logger = get_logger(os.path.join(ROOT, "hft/logs", "producer"), "producer")

        # data
        self.kline_1s_data = None
        self.kline_1m_data = None
        self.orderbook_data = None
        self.previous_kline_1s_data = None
        self.previous_kline_1m_data = None
        self.previous_orderbook_data = None

        # timestamp
        self.orderbook_timestep = 0
        self.kline_1s_timestep = 0
        self.kline_1m_timestep = 0
        self.last_minute = 0

        self.orderbook_queue = queue.Queue()
        self.kline_1s_queue = queue.Queue()
        self.kline_1m_queue = queue.Queue()

        self.orderbook_wss = OrderBookWebsocketStream(args = args, queue=self.orderbook_queue, logger=self.logger).start()
        self.kline_1s_wss = KlineWebsocketStream(args = args, interval="1s", queue=self.kline_1s_queue, logger=self.logger).start()
        self.kline_1m_wss = KlineWebsocketStream(args = args, interval="1m", queue=self.kline_1m_queue, logger=self.logger).start()

    def store_data_in_rabbitmq(self, combined_data):
        channel = pika_connection.channel()

        # init queue
        channel.queue_declare(queue='binance_data')

        # to json string
        message = json.dumps(combined_data)

        # push a message
        channel.basic_publish(exchange='',
                              routing_key='binance_data',
                              body=message)
        self.logger.info("Stored data in RabbitMQ, Order book timestep: {}, Kline 1s timestep: {}, Kline 1m timestep: {}".format(self.orderbook_timestep, self.kline_1s_timestep, self.kline_1m_timestep))

        channel.close()

    def run(self):

        while True:
            self.orderbook_data = self.orderbook_queue.get()
            self.orderbook_timestep = self.orderbook_data["timestep"] // 1000
            self.kline_1s_data = self.kline_1s_queue.get()
            self.kline_1s_timestep = self.kline_1s_data["timestep"] // 1000

            max_timestep = max(self.kline_1s_timestep, self.orderbook_timestep)

            # print(f"max_timestep : {max_timestep}")

            # while (not self.orderbook_queue.empty()) and self.orderbook_timestep < max_timestep:
            while self.orderbook_timestep < max_timestep:
                self.orderbook_data = self.orderbook_queue.get()
                self.orderbook_timestep = self.orderbook_data["timestep"] // 1000

            # print("orderbook aligned")

            # while (not self.kline_1s_queue.empty()) and self.kline_1s_timestep < max_timestep:
            while self.kline_1s_timestep < max_timestep:
                self.kline_1s_data = self.kline_1s_queue.get()
                self.kline_1s_timestep = self.kline_1s_data["timestep"] // 1000

            # print("kline_1s aligned")

            while (not self.kline_1m_queue.empty()) and self.kline_1m_timestep < max_timestep:
            # while self.kline_1m_timestep < max_timestep:
                self.kline_1m_data = self.kline_1m_queue.get()
                self.kline_1m_timestep = self.kline_1m_data["timestep"] // 1000

            self.logger.info("****{}_{}_{}".format(self.orderbook_timestep, self.kline_1s_timestep, self.kline_1m_timestep))

            if self.orderbook_timestep == self.kline_1s_timestep:
                combined_data = {
                    'interval': '1s',
                    'orderbook': self.orderbook_data,
                    'kline_1s': self.kline_1s_data
                }
                # print(combined_data)
                self.store_data_in_rabbitmq(combined_data)
            # else:
            #     time.sleep(1)
            
            minute = (self.kline_1m_timestep - 1) // 60
            
            if minute > self.last_minute:
                combined_data = {
                    'interval': '1m',
                    'kline_1m': self.kline_1m_data
                }
                # print(combined_data)
                self.last_minute = minute
                self.store_data_in_rabbitmq(combined_data)
                

@app.task
def restart_producer():
    args = get_args_parser()
    producer = Producer(args)
    producer.start()

if __name__ == "__main__":
    args = get_args_parser()
    producer = Producer(args)
    producer.start()