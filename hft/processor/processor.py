import sys
from collections import namedtuple
import pandas as pd
import numpy as np

EPS = sys.float_info.epsilon

class Processor():
    def __init__(self, beat_fee = 0.0001, windows = [5,10,30,60]):
        self.beat_fee = beat_fee
        self.windows = windows

    def run(self, kline_df, orderbook_df):
        data = {}
        order_features = self.process_orderbook(orderbook_df)
        kline_1s_features = self.process_kline_1s(kline_df["s"])
        kline_1m_features = self.process_kline_1m(kline_df["m"])

        data.update(order_features)
        data.update(kline_1s_features)
        data.update(kline_1m_features)

        return data

    def process_orderbook(self, _df: pd.DataFrame):
        df = _df.copy()
        beat_fee = self.beat_fee
        windows = self.windows

        real_min = 1e-5
        df["midpoint"] = (df["ask1_price"] + df["bid1_price"]) / 2
        df["buy_volume_oe"] = (df["bid1_size"] + df["bid2_size"] +
                            df["bid3_size"] + df["bid4_size"] + df["bid5_size"])
        df["sell_volume_oe"] = (df["ask1_size"] + df["ask2_size"] +
                                df["ask3_size"] + df["ask4_size"] +
                                df["ask5_size"])
        df["bid1_size_n"] = df["bid1_size"] / df["buy_volume_oe"]
        df["bid2_size_n"] = df["bid2_size"] / df["buy_volume_oe"]
        df["bid3_size_n"] = df["bid3_size"] / df["buy_volume_oe"]
        df["bid4_size_n"] = df["bid4_size"] / df["buy_volume_oe"]
        df["bid5_size_n"] = df["bid5_size"] / df["buy_volume_oe"]
        df["ask1_size_n"] = df["ask1_size"] / df["sell_volume_oe"]
        df["ask2_size_n"] = df["ask2_size"] / df["sell_volume_oe"]
        df["ask3_size_n"] = df["ask3_size"] / df["sell_volume_oe"]
        df["ask4_size_n"] = df["ask4_size"] / df["sell_volume_oe"]
        df["ask5_size_n"] = df["ask5_size"] / df["sell_volume_oe"]
        df["buy_spread_oe"] = np.abs(df["bid1_price"] - df["bid5_price"])
        df["sell_spread_oe"] = np.abs(df["ask1_price"] - df["ask5_price"])
        df["imblance_volume_oe"] = (df["buy_volume_oe"] - df["sell_volume_oe"]) / (
            df["buy_volume_oe"] + df["sell_volume_oe"])
        df["oe_spread_1"] = np.abs(df["bid1_price"] - df["ask1_price"])
        df["oe_spread_5"] = np.abs(df["bid5_price"] - df["ask5_price"])
        df["wap_1"] = (df["bid1_price"] * df["ask1_size"] +
                    df["ask1_price"] * df["bid1_size"]) / (
                        df["ask1_size"] + df["bid1_size"] + real_min)
        df["wap_2"] = (df["bid2_price"] * df["ask2_size"] +
                    df["ask2_price"] * df["bid2_size"]) / (
                        df["ask2_size"] + df["bid2_size"] + real_min)
        df["wap_balance"] = df["wap_1"] - df["wap_2"]
        df["sell_vwap"] = df["ask1_size_n"] * df["ask1_price"] + df[
            "ask2_size_n"] * df["ask2_price"] + df["ask3_size_n"] * df[
                "ask3_price"] + df["ask4_size_n"] * df["ask4_price"] + df[
                    "ask5_size_n"] * df["ask5_price"]
        df["buy_vwap"] = df["bid1_size_n"] * df["bid1_price"] + df[
            "bid2_size_n"] * df["bid2_price"] + df["bid3_size_n"] * df[
                "bid3_price"] + df["bid4_size_n"] * df["bid4_price"] + df[
                    "bid5_size_n"] * df["bid5_price"]
        df["sell_buy_vwap_spread"] = df["buy_vwap"] - df["sell_vwap"]
        EPS = df["bid1_price"] * beat_fee
        df.drop(columns=["sell_volume_oe", "buy_volume_oe"], inplace=True)

        for w in windows:
            midpoint_rolling = df["midpoint"].rolling(w)
            ask1_rolling = df["ask1_price"].rolling(w)
            bid1_rolling = df["bid1_price"].rolling(w)
            df["midpoint_diff_{}".format(w)] = df["midpoint"].diff(
                periods=w) / (midpoint_rolling.std() + EPS)
            df["ask1_diff_{}".format(w)] = df["ask1_price"].diff(
                periods=w) / (ask1_rolling.std() + EPS)
            df["bid1_diff_{}".format(w)] = df["bid1_price"].diff(
                periods=w) / (bid1_rolling.std() + EPS)
            df["ask2_diff_{}".format(w)] = df["ask2_price"].diff(
                periods=w) / (df["ask2_price"].rolling(w).std() + EPS)
            df["bid2_diff_{}".format(w)] = df["bid2_price"].diff(
                periods=w) / (df["bid2_price"].rolling(w).std() + EPS)
            buy_spread_rolling = df["buy_spread_oe"].rolling(w)
            sell_spread_rolling = df["sell_spread_oe"].rolling(w)
            imblance_volume_rolling = df["imblance_volume_oe"].rolling(w)
            wap_balance_rolling = df["wap_balance"].rolling(w)
            for name, rolling_result in zip([
                    "midpoint", "bid1_price", "ask1_price", "buy_spread_oe",
                    "sell_spread_oe", "imblance_volume_oe", "wap_balance"
            ], [
                    midpoint_rolling,
                    bid1_rolling,
                    ask1_rolling,
                    buy_spread_rolling,
                    sell_spread_rolling,
                    imblance_volume_rolling,
                    wap_balance_rolling,
            ]):
                df["{}_rolling_mean_{}".format(name, w)] = rolling_result.mean()
                df["{}_std_{}".format(name, w)] = rolling_result.std() + EPS
                df["{}_trend_{}".format(
                    name,
                    w)] = (df[name] - df["{}_rolling_mean_{}".format(name, w)]) / (
                        df["{}_std_{}".format(name, w)] + EPS)
                df["{}_diff_{}".format(name, w)] = (df[name].diff(
                    periods=w)) / (df["{}_std_{}".format(name, w)] + EPS)
                df.drop(columns=[
                    "{}_rolling_mean_{}".format(name, w),
                    "{}_std_{}".format(name, w),
                ],
                        inplace=True)
        df = df.iloc[max(windows):].reset_index(drop=True)
        return df.iloc[-1].to_dict()
    
    def process_kline_1s(self, _df: pd.DataFrame):
        df = _df.copy()
        beat_fee = self.beat_fee
        windows = self.windows

        EPS = df["close_s"] * beat_fee
        df["max_oc_s"] = df[["open_s", "close_s"]].max(axis=1)
        df["min_oc_s"] = df[["open_s", "close_s"]].min(axis=1)
        df["kmid_s"] = (df["close_s"] - df["open_s"])
        df["klen_s"] = (df["high_s"] - df["low_s"])
        df["kmid2_s"] = (df["close_s"] - df["open_s"]) / (df["high_s"] -
                                                        df["low_s"] + EPS)
        df["kup_s"] = (df["high_s"] - df["max_oc_s"])
        df["kup2_s"] = (df["high_s"] - df["max_oc_s"]) / (df["high_s"] -
                                                        df["low_s"] + EPS)
        df["klow_s"] = (df["min_oc_s"] - df["low_s"])
        df["klow2_s"] = (df["min_oc_s"] - df["low_s"]) / (df["high_s"] -
                                                        df["low_s"] + EPS)
        df["ksft_s"] = (2 * df["close_s"] - df["high_s"] - df["low_s"])
        df["ksft2_s"] = (2 * df["close_s"] - df["high_s"] -
                        df["low_s"]) / (df["high_s"] - df["low_s"] + EPS)
        df.drop(columns=["max_oc_s", "min_oc_s"])
        for w in windows:
            close_rolling = df["close_s"].rolling(w)
            low_rolling = df["low_s"].rolling(w)
            high_rolling = df["high_s"].rolling(w)
            close_shift = df["close_s"].shift(w)
            df["rsv_{}_s".format(w)] = (df["close_s"] - low_rolling.min()) / (
                (high_rolling.max() - low_rolling.min()) + EPS)

            df["std_{}_close_s".format(w)] = close_rolling.std() / df["close_s"]
            df["std_{}_s".format(w)] = close_rolling.std() + EPS
            df["ma_{}_s".format(w)] = (close_rolling.mean() - df["close_s"]) / (
                df["std_{}_s".format(w)] + EPS)
            df["roc_{}_s".format(w)] = (close_shift - df["close_s"]) / (
                df["std_{}_s".format(w)] + EPS)

            df["beta_{}_s".format(w)] = (close_shift - df["close_s"]) / (
                w * (df["std_{}_s".format(w)] + EPS))

            df["max_{}_s".format(w)] = (close_rolling.max() - df["close_s"]) / (
                df["std_{}_s".format(w)] + EPS)

            df["min_{}_s".format(w)] = (close_rolling.min() - df["close_s"]) / (
                df["std_{}_s".format(w)] + EPS)
            df["qtlu_{}_s".format(
                w)] = (close_rolling.quantile(0.8) -
                    df["close_s"]) / (df["std_{}_s".format(w)] + EPS)

            df["qtld_{}_s".format(
                w)] = (close_rolling.quantile(0.2) -
                    df["close_s"]) / (df["std_{}_s".format(w)] + EPS)
            df["rsv_{}_s".format(w)] = (df["close_s"] - low_rolling.min()) / (
                (high_rolling.max() - low_rolling.min()) + EPS)

            df["imax_{}_s".format(w)] = high_rolling.apply(np.argmax) / w

            df["imin_{}_s".format(w)] = low_rolling.apply(np.argmin) / w

            df["imxd_{}_s".format(w)] = (high_rolling.apply(np.argmax) -
                                        low_rolling.apply(np.argmin)) / w
            df["imxd_{}_dis_s".format(w)] = (df["imxd_{}_s".format(w)] >
                                            0).astype('int')
        df = df.iloc[max(windows):].reset_index(drop=True)
        return df.iloc[-1].to_dict()
    
    def process_kline_1m(self, _df: pd.DataFrame):
        df = _df.copy()
        beat_fee = self.beat_fee
        windows = self.windows
        
        EPS = df["close_m"] * beat_fee
        df["max_oc_m"] = df[["open_m", "close_m"]].max(axis=1)
        df["min_oc_m"] = df[["open_m", "close_m"]].min(axis=1)
        df["kmid_m"] = (df["close_m"] - df["open_m"])
        df["klen_m"] = (df["high_m"] - df["low_m"])
        df["kmid2_m"] = (df["close_m"] - df["open_m"]) / (df["high_m"] -
                                                        df["low_m"] + EPS)
        df["kup_m"] = (df["high_m"] - df["max_oc_m"])
        df["kup2_m"] = (df["high_m"] - df["max_oc_m"]) / (df["high_m"] -
                                                        df["low_m"] + EPS)
        df["klow_m"] = (df["min_oc_m"] - df["low_m"])
        df["klow2_m"] = (df["min_oc_m"] - df["low_m"]) / (df["high_m"] -
                                                        df["low_m"] + EPS)
        df["ksft_m"] = (2 * df["close_m"] - df["high_m"] - df["low_m"])
        df["ksft2_m"] = (2 * df["close_m"] - df["high_m"] -
                        df["low_m"]) / (df["high_m"] - df["low_m"] + EPS)
        df.drop(columns=["max_oc_m", "min_oc_m"])
        for w in windows:
            close_rolling = df["close_m"].rolling(w)
            low_rolling = df["low_m"].rolling(w)
            high_rolling = df["high_m"].rolling(w)
            close_shift = df["close_m"].shift(w)
            df["rsv_{}_m".format(w)] = (df["close_m"] - low_rolling.min()) / (
                (high_rolling.max() - low_rolling.min()) + EPS)

            df["std_{}_close_m".format(w)] = close_rolling.std() / df["close_m"]
            df["std_{}_m".format(w)] = close_rolling.std() + EPS
            df["ma_{}_m".format(w)] = (close_rolling.std() -
                                    df["close_m"]) / (close_rolling.std() + EPS)
            df["roc_{}_m".format(
                w)] = (close_shift - df["close_m"]) / (close_rolling.std() + EPS)

            df["beta_{}_m".format(
                w)] = (close_shift - df["close_m"]) / (w *
                                                    (close_rolling.std() + EPS))

            df["max_{}_m".format(w)] = (close_rolling.max() - df["close_m"]) / (
                close_rolling.std() + EPS)

            df["min_{}_m".format(w)] = (close_rolling.min() - df["close_m"]) / (
                close_rolling.std() + EPS)
            df["qtlu_{}_m".format(w)] = (close_rolling.quantile(0.8) -
                                        df["close_m"]) / (close_rolling.std() +
                                                        EPS)

            df["qtld_{}_m".format(w)] = (close_rolling.quantile(0.2) -
                                        df["close_m"]) / (close_rolling.std() +
                                                        EPS)
            df["rsv_{}_m".format(w)] = (df["close_m"] - low_rolling.min()) / (
                (high_rolling.max() - low_rolling.min()) + EPS)

            df["imax_{}_m".format(w)] = high_rolling.apply(np.argmax) / w

            df["imin_{}_m".format(w)] = low_rolling.apply(np.argmin) / w

            df["imxd_{}_m".format(w)] = (high_rolling.apply(np.argmax) -
                                        low_rolling.apply(np.argmin)) / w
            df["imxd_{}_dis_m".format(w)] = (df["imxd_{}_m".format(w)] >
                                            0).astype('int')
        df = df.iloc[max(windows):].reset_index(drop=True)
        return df.iloc[-1].to_dict()