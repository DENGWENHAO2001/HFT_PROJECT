from hft.agents.model.net import *
from torch.nn import functional as F
from torch import nn
import numpy as np
import torch
import os
import sys
import pathlib
from datetime import datetime
from hft.logger import get_logger

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(ROOT)
sys.path.insert(0, ".")

MAX_PUNISH = 1e12

model_path_list_dict = {
    0: [
        "result_risk/BTCUSDT/potential_model/initial_action_0/model_0.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_0/model_1.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_0/model_2.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_0/model_3.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_0/model_4.pth",
    ],
    1: [
        "result_risk/BTCUSDT/potential_model/initial_action_1/model_0.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_1/model_1.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_1/model_2.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_1/model_3.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_1/model_4.pth",
    ],
    2: [
        "result_risk/BTCUSDT/potential_model/initial_action_2/model_0.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_2/model_1.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_2/model_2.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_2/model_3.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_2/model_4.pth",
    ],
    3: [
        "result_risk/BTCUSDT/potential_model/initial_action_3/model_0.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_3/model_1.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_3/model_2.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_3/model_3.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_3/model_4.pth",
    ],
    4: [
        "result_risk/BTCUSDT/potential_model/initial_action_4/model_0.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_4/model_1.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_4/model_2.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_4/model_3.pth",
        "result_risk/BTCUSDT/potential_model/initial_action_4/model_4.pth",
    ],
}
class HFTDDQN():
    def __init__(self, checkpoint_path):
        self.position = .0  # previous action
        self.max_holding_number = 0.01
        self.logger = get_logger(os.path.join(ROOT, "hft/logs", "hft_ddqn"), "hft_ddqn")
        self.logger.propagate = False
        self.device = torch.device(
            "cuda:0" if torch.cuda.is_available() else "cpu")

        # TODO 把这个结果转成get_feature类的结果
        self.minute_tech_indicator_list = np.load(os.path.join(
            ROOT, "hft", "agents", "feature", "test_minitue_feature.npy"), allow_pickle=True)
        self.second_tech_indicator_list = np.load(os.path.join(
            ROOT, "hft", "agents", "feature", "test_second_feature.npy"), allow_pickle=True)
        # TODO 第一个就是high level的模型初始化以及load trained的model
        # TODO low level 多个魔心固定初始化已经load model 并形成对应的value为list（内涵加载的模型）的字典
        self.high_level_agent = Qnet_high_level_position(int(len(self.minute_tech_indicator_list)), 5, 128).to("cpu")

        self.low_level_agent_list_dict = {}
        for key in model_path_list_dict:
            self.low_level_agent_list_dict[key] = []
            for model_path in model_path_list_dict[key]:
                model = Qnet(
                    int(len(self.second_tech_indicator_list)), 5, 128).to("cpu")
                model.load_state_dict(
                    torch.load(
                        os.path.dirname(os.path.abspath(__file__))+"/"+model_path,
                        map_location=torch.device("cpu"),
                    )
                )
                self.low_level_agent_list_dict[key].append(model)


        self.chosen_model = self.low_level_agent_list_dict[0][0]




    def get_features(self, features):
        minute_feature = np.array([features[item]
                                  for item in self.minute_tech_indicator_list])
        minute_feature = torch.tensor(
            minute_feature, dtype=torch.float32).to(self.device)
        minute_feature = minute_feature.unsqueeze(0)

        second_feature = np.array([features[item]
                                  for item in self.second_tech_indicator_list])
        second_feature = torch.tensor(
            second_feature, dtype=torch.float32).to(self.device)
        second_feature = second_feature.unsqueeze(0)

        return minute_feature, second_feature

    def get_previous_action(self):
        return torch.tensor([1000 * self.position], dtype=torch.long).to(self.device)

    def set_previous_action(self, previous_action):
        self.position = previous_action * 0.001

    def get_buy_size_max(self, price_information):

        ask1_size = price_information["ask1_size"]
        ask2_size = price_information["ask2_size"]
        ask3_size = price_information["ask3_size"]
        ask4_size = price_information["ask4_size"]

        return ask1_size + ask2_size + ask3_size + ask4_size

    def get_sell_size_max(self, price_information):

        bid1_size = price_information["bid1_size"]
        bid2_size = price_information["bid2_size"]
        bid3_size = price_information["bid3_size"]
        bid4_size = price_information["bid4_size"]

        return bid1_size + bid2_size + bid3_size + bid4_size

    def get_avaliable_action(self, price_information):

        buy_size_max = self.get_buy_size_max(price_information)
        sell_size_max = self.get_sell_size_max(price_information)

        position_upper = self.position + buy_size_max
        position_lower = self.position - sell_size_max
        position_lower = max(position_lower, 0)
        position_upper = min(position_upper, self.max_holding_number)

        # transfer the position back into our action
        current_action = 1000 * self.position

        action_upper = int(position_upper * 10 / self.max_holding_number)
        if position_lower == 0:
            action_lower = 0
        else:
            action_lower = min(
                int(position_lower * 10 / self.max_holding_number) + 1,
                action_upper, current_action)
        avaliable_discriminator = []
        for i in range(5):
            if i >= action_lower and i <= action_upper:
                avaliable_discriminator.append(1)
            else:
                avaliable_discriminator.append(0)
        avaliable_discriminator = torch.tensor(
            avaliable_discriminator, dtype=torch.float32).to(self.device)
        return avaliable_discriminator

    def run(self, features, price_information):
        current_timestamp = int(price_information["kline_1s"]["timestep"])//1000
        current_timestamp = datetime.utcfromtimestamp(current_timestamp)
        # TODO 把对饮固定秒数提出来 如果不等于59

        minute_feature, second_feature = self.get_features(features)
        previous_action = self.get_previous_action().float().unsqueeze(1)
        avaliable_action = self.get_avaliable_action(price_information["orderbook"]).reshape(1, 5)

        if current_timestamp.second != 59:
            output_action=self.chosen_model(
                second_feature, previous_action, avaliable_action).argmax(dim=1, keepdim=True)



        else:

            max_value_index = self.high_level_agent(
                minute_feature, previous_action).argmax(dim=1, keepdim=True)
            self.chosen_model = self.low_level_agent_list_dict[int(previous_action.item())][max_value_index.item()]
            output_action = self.chosen_model(second_feature, previous_action, avaliable_action).argmax(dim=1, keepdim=True)

        output_action = output_action.item()
        self.set_previous_action(output_action)
        print(1)
        self.logger.info(
            "timestamp:{},position:{},output_action:{},price_info:{}"
            .format(current_timestamp, previous_action.item()*self.max_holding_number/4,output_action,{k: v for k, v in price_information["orderbook"].items() if k !="timestep"}))

        return output_action


MODEL = HFTDDQN(checkpoint_path=os.path.join(
    ROOT, "hft", "agents", "trained_model.pkl"))


def get_action_infos(features, price_information):
    infos = MODEL.run(features, price_information)
    return infos
