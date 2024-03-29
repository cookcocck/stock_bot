import os
import time
import random
import warnings
import datetime
import numpy as np
import pandas as pd
import akshare as ak
import pandas_ta as ta
from loguru import logger
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait


class StockBot:
    def __init__(self) -> None:
        self.m_symbols = []
        self.m_macd_config = {"fast": 12, "slow": 26, "signal": 9}

    def get_symbols(self):
        df = ak.stock_zh_a_spot_em()
        self.m_symbols = df["代码"]

    def __clear_log():
        if os.path.exists("result.log"):
            with open("result.log", "r+") as f:
                f.seek(0)
                f.truncate()

    @staticmethod
    def __dkx_cross_strategy(
        symbols: list, period: str, start_date: str, end_date: str, macd_config: dict
    ):
        for _, symbol in enumerate(symbols):
            try:
                logger.info("check {0}".format(symbol))
                time.sleep(random.choice([0.2, 0.3, 0.4, 0.5]))

                # get symbol data
                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                )

                # filter new stocks
                if df.shape[0] < 100:
                    continue

                df = df.drop(
                    columns=[
                        "日期",
                        "成交量",
                        "成交额",
                        "振幅",
                        "涨跌幅",
                        "涨跌额",
                        "换手率",
                    ]
                )

                # transform to 3d period
                blocks = -1
                if df.shape[0] % 3 == 0:
                    blocks = df.shape[0] / 3
                else:
                    blocks = df.shape[0] / 3 + 1
                split_dfs = np.array_split(df.copy(), blocks)
                df = pd.DataFrame(columns=["开盘", "收盘", "最高", "最低"])
                for split_df in split_dfs:
                    df.loc[len(df)] = [
                        split_df[:1]["开盘"].to_list()[0],
                        split_df[-1:]["收盘"].to_list()[0],
                        split_df["最高"].max(),
                        split_df["最低"].min(),
                    ]

                df = df.iloc[::-1]
                df.reset_index(drop=True, inplace=True)

                # calculate dkx
                dkx = np.ones(df.shape[0])
                for idx in range(0, df.shape[0]):
                    sum = 0
                    count = 0
                    if df.iloc[idx : idx + 20].shape[0] == 20:
                        for _, row in df.iloc[idx : idx + 20].iterrows():
                            sum += (20 - count) * (
                                (
                                    3 * row["收盘"]
                                    + row["最低"]
                                    + row["开盘"]
                                    + row["最高"]
                                )
                                / 6
                            )
                            count += 1
                    sum /= 210
                    dkx[idx] = sum

                df["dkx"] = dkx.tolist()

                # calculate dkx_ma
                dkx_sma = np.ones(df.shape[0])
                for idx in range(0, df.shape[0]):
                    sum = 0
                    if df.iloc[idx : idx + 10].shape[0] == 10:
                        for _, row in df.iloc[idx : idx + 10].iterrows():
                            sum += row["dkx"]
                    sum /= 10
                    dkx_sma[idx] = sum
                df["dkx_sma"] = dkx_sma.tolist()

                df = df.iloc[::-1]
                df.reset_index(drop=True, inplace=True)

                # check dkx crossing up dkx_sma
                if (
                    df.iloc[-2]["dkx"] < df.iloc[-2]["dkx_sma"]
                    and df.iloc[-1]["dkx"] > df.iloc[-1]["dkx_sma"]
                ):
                    # check macd and signal
                    macd_df = ta.macd(
                        df["收盘"],
                        macd_config["fast"],
                        macd_config["slow"],
                        macd_config["signal"],
                    )
                    if (
                        macd_df.iloc[-1]["MACD_12_26_9"] > 0
                        and macd_df.iloc[-1]["MACDs_12_26_9"] > 0
                    ) and (
                        macd_df.iloc[-1]["MACD_12_26_9"]
                        > macd_df.iloc[-1]["MACDs_12_26_9"]
                    ):
                        logger.success("{0} Cross Up !".format(symbol))
            except Exception as error:
                logger.error("process {0} error: {1}!".format(symbol, error))

    def dkx_cross_strategy(self, period: str, start_date: str, end_date: str):
        StockBot.__clear_log()
        logger.add("result.log")
        logger.info("start checking...")
        max_workers = min(32, os.cpu_count() + 4)
        split_symbols = [
            self.m_symbols[i : i + int(len(self.m_symbols) / max_workers)]
            for i in range(
                0, len(self.m_symbols), int(len(self.m_symbols) / max_workers)
            )
        ]
        with ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="stock_bot_"
        ) as pool:
            all_task = [
                pool.submit(
                    StockBot.__dkx_cross_strategy,
                    split_symbols[i],
                    period,
                    start_date,
                    end_date,
                    self.m_macd_config,
                )
                for i in range(0, len(split_symbols))
            ]
            wait(all_task, return_when=ALL_COMPLETED)
            logger.info("check finished")


if __name__ == "__main__":
    warnings.filterwarnings("ignore")  # for numpy and pandas deprecated functions
    stock_bot = StockBot()
    stock_bot.get_symbols()
    today = datetime.datetime.now().strftime("%Y%m%d")
    last_year_tody = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime(
        "%Y%m%d"
    )
    stock_bot.dkx_cross_strategy(
        period="daily", start_date=last_year_tody, end_date=today
    )
