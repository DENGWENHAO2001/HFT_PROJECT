import json
import os
import sys
import numpy as np
from celery import shared_task
import pandas as pd
import argparse
import yaml
from datetime import datetime
ROOT = os.path.dirname(os.path.abspath(__file__))#os.path.dirname(os.path.dirname(__file__))
sys.path.append(ROOT)
sys.path.insert(0, "hft/tools")

from hft.processor import Processor
# from hft.agents import get_action_infos
from hft.rabbitmq import pika_connection
from hft.utils import dict_to_args
from hft.logger import get_logger
from hft.agents.hft_ddqn import HFTDDQN

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

class Consumer():
    def __init__(self, args, cache_kline_windows = [5,10,30,60]):
        self.args = args
        self.agent = HFTDDQN(self.args)
        self.logger = get_logger(os.path.join(ROOT, "hft/logs", "consumer"), "consumer")
        self.logger_ddqn = get_logger(os.path.join(ROOT, "hft/logs", "trader"), "trader")

        self.logger_feature = get_logger(os.path.join(ROOT, "hft/logs", "consumer"), "feature")





        self.cache_kline_windows = cache_kline_windows
        self.max_kline_windows = max(cache_kline_windows)

        self.cache_kline_df = {
            "s": pd.DataFrame(data=None, columns=["open_s", "high_s", "low_s", "close_s"]),
            "m": pd.DataFrame(data=None, columns=["open_m", "high_m", "low_m", "close_m"])
        }
        self.cache_orderbook_df = pd.DataFrame(data=None, columns=["bid1_price", "bid1_size",
                                                                   "bid2_price", "bid2_size",
                                                                   "bid3_price", "bid3_size",
                                                                   "bid4_price", "bid4_size",
                                                                   "bid5_price", "bid5_size",
                                                                   "ask1_price", "ask1_size",
                                                                   "ask2_price", "ask2_size",
                                                                   "ask3_price", "ask3_size",
                                                                   "ask4_price", "ask4_size",
                                                                   "ask5_price", "ask5_size"])
        
        self.processor = Processor(args.beat_fee, cache_kline_windows)

    def cache_kline_data(self, interval, kline_data):
        row = pd.DataFrame(data={f"open_{interval}": kline_data["open"], f"high_{interval}": kline_data["high"], 
                                 f"low_{interval}": kline_data["low"], f"close_{interval}": kline_data["close"]}, index=[len(self.cache_kline_df[interval])])
        self.cache_kline_df[interval] = pd.concat([self.cache_kline_df[interval], row], ignore_index=True)

        if len(self.cache_kline_df[interval]) >= self.max_kline_windows + 1:
            self.cache_kline_df[interval] = self.cache_kline_df[interval].iloc[-(self.max_kline_windows + 1):, :]
    
    def cache_orderbook_data(self, orderbook_data):
        row = pd.DataFrame(data=orderbook_data, index=[len(self.cache_orderbook_df)])
        self.cache_orderbook_df = pd.concat([self.cache_orderbook_df, row], ignore_index=True)

        if len(self.cache_orderbook_df) >= self.max_kline_windows + 1:
            self.cache_orderbook_df = self.cache_orderbook_df.iloc[-(self.max_kline_windows + 1):, :]

    # def append_cache_kline_data(self, data):  # needs to be updated (the output should match the process input)
    #     kline_data = data["kline"]

    #     append_kline_data = {}
    #     for w in self.cache_kline_windows:
    #         rolling_df = self.cache_kline_df[["open","high","low","close"]].rolling(w)
    #         ret1 = self.cache_kline_df["close"].pct_change(1)

    #         # close
    #         close_shift_w = self.cache_kline_df["close"].shift(w).iloc[-1]
    #         append_kline_data[f"close_shift_{w}"] = close_shift_w
    #         close_rolling_mean_w = rolling_df["close"].mean().iloc[-1]
    #         append_kline_data[f"close_rolling_mean_{w}"] = close_rolling_mean_w
    #         close_rolling_std_w = rolling_df["close"].std().iloc[-1]
    #         append_kline_data[f"close_rolling_std_{w}"] = close_rolling_std_w
    #         close_rolling_max_w = rolling_df["close"].max().iloc[-1]
    #         append_kline_data[f"close_rolling_max_{w}"] = close_rolling_max_w
    #         close_rolling_min_w = rolling_df["close"].min().iloc[-1]
    #         append_kline_data[f"close_rolling_min_{w}"] = close_rolling_min_w
    #         close_rolling_qtlu_w = rolling_df["close"].quantile(0.8).iloc[-1]
    #         append_kline_data[f"close_rolling_qtlu_{w}"] = close_rolling_qtlu_w
    #         close_rolling_qtld_w = rolling_df["close"].quantile(0.2).iloc[-1]
    #         append_kline_data[f"close_rolling_qtld_{w}"] = close_rolling_qtld_w

    #         # low
    #         low_rolling_min_w = rolling_df["low"].min().iloc[-1]
    #         append_kline_data[f"low_rolling_min_{w}"] = low_rolling_min_w
    #         low_rolling_imin_w = rolling_df["low"].apply(np.argmin).iloc[-1]
    #         append_kline_data[f"low_rolling_imin_{w}"] = low_rolling_imin_w

    #         # high
    #         high_rolling_max_w = rolling_df["high"].max().iloc[-1]
    #         append_kline_data[f"high_rolling_max_{w}"] = high_rolling_max_w
    #         high_rolling_imax_w = rolling_df["high"].apply(np.argmax).iloc[-1]
    #         append_kline_data[f"high_rolling_imax_{w}"] = high_rolling_imax_w

    #         cntp_w = (ret1.gt(0)).rolling(w).sum().iloc[-1]
    #         append_kline_data[f"cntp_{w}"] = cntp_w

    #         cntn_w = (ret1.lt(0)).rolling(w).sum().iloc[-1]
    #         append_kline_data[f"cntn_{w}"] = cntn_w

    #     kline_data.update(append_kline_data)
    #     data["kline"] = kline_data
    #     return data

    def trans2float_1s(self, data):             # transfer str to float for 1s_data
        orderbook_data = data["orderbook"]
        kline_data = data["kline_1s"]
        for k,v in orderbook_data.items():
            orderbook_data[k] = float(v)
        for k,v in kline_data.items():
            kline_data[k] = float(v)
        data["orderbook"] = orderbook_data
        data["kline_1s"] = kline_data
        return data

    def trans2float_1m(self, data):             # transfer str to float for 1m_data
        kline_data = data["kline_1m"]
        for k,v in kline_data.items():
            kline_data[k] = float(v)
        data["kline_1m"] = kline_data
        return data

    def start_consumer(self):
        channel = pika_connection.channel()
        channel.queue_declare(queue='binance_data')
        channel.basic_qos(prefetch_count=1)

        def callback(ch, method, properties, body):
            data = json.loads(body)
            
            if data['interval'] == '1s':
                # convert to float
                data = self.trans2float_1s(data)
                orderbook_timestep = str(int(data["orderbook"]["timestep"]) // 1000)
                kline_1s_timestep = str(int(data["kline_1s"]["timestep"]) // 1000)

                self.logger.info("Received data from RabbitMQ: Order book timestep: {}, Kline_1s timestep: {}".format(orderbook_timestep, kline_1s_timestep))

                self.cache_kline_data('s', data["kline_1s"])
                self.cache_orderbook_data(data["orderbook"])

            else:
                # convert to float
                data = self.trans2float_1m(data)
                kline_1m_timestep = str(int(data["kline_1m"]["timestep"]) // 1000)
                
                self.logger.info("Received data from RabbitMQ: Kline_1m timestep: {}".format(kline_1m_timestep))

                self.cache_kline_data('m', data["kline_1m"])

            # print()

            if len(self.cache_kline_df['m']) >= self.max_kline_windows + 1 and data['interval'] == '1s':
                features = self.processor.run(self.cache_kline_df, self.cache_orderbook_df)



                action_infos ,previous_action= self.agent.run(features,data)

                timestamp=int(data["kline_1s"]["timestep"]) // 1000  #

                self.logger_ddqn.info(
                    "timestamp:{},position:{},output_action:{},price_info:{}"
                    .format(datetime.utcfromtimestamp(timestamp), previous_action * 0.01 / 4, action_infos,{k: v for k, v in data["orderbook"].items() if k != "timestep"}))#
                self.logger.info(
                "Action info: {}, Kline timestep: {}"
                .format(action_infos,datetime.utcfromtimestamp(data["kline_1s"]["timestep"]//1000) ))#kline_1m_timestep

             
        channel.basic_consume(
            queue='binance_data',
            on_message_callback=callback,
            auto_ack=True)

        self.logger.info('Waiting for message.')
        channel.start_consuming()

@shared_task
def consume_data():
    args = get_args_parser()
    consumer = Consumer(args)
    consumer.start_consumer()

if __name__ == "__main__":
    args = get_args_parser()
    consumer = Consumer(args)
    consumer.start_consumer()