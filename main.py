import akshare as ak
import numpy as np
from loguru import logger
import time
import random
import pandas_ta as ta

class StockBot:
    def __init__(self) -> None:
        self.m_symbols = []
        self.m_macd_config = {
            'fast': 12,
            'slow': 26,
            'signal': 9
        }

    def get_symbols(self) -> list[str]:
        df = ak.stock_zh_a_spot_em()
        self.m_symbols = df['代码']

    def dkx_cross_strategy(self, period: str, start_date: str, end_date: str):
        for symbol in self.m_symbols:
            try:
                time.sleep(random.choice([0.2, 0.3, 0.4, 0.5]))

                # get symbol data
                df = ak.stock_zh_a_hist(symbol=symbol, period=period, start_date=start_date, end_date=end_date, adjust="qfq")
                
                # filter new stocks
                if df.shape[0] < 100:
                    continue
                
                df = df.iloc[::-1]
                df.reset_index(drop=True, inplace=True)

                # calculate dkx
                dkx = np.ones(df.shape[0])
                for idx in range(0, df.shape[0]):
                    sum = 0
                    count = 0
                    if df.iloc[idx: idx + 20].shape[0] == 20:
                        for _, row in df.iloc[idx: idx + 20].iterrows():
                            sum += (20 - count) * ((3 * row['收盘'] + row['最低'] + row['开盘'] + row['最高']) / 6)
                            count += 1
                    sum /= 210
                    dkx[idx] = sum

                df['dkx'] = dkx.tolist()

                # calculate dkx_ma
                dkx_sma = np.ones(df.shape[0])
                for idx in range(0, df.shape[0]):
                    sum = 0
                    if df.iloc[idx: idx + 10].shape[0] == 10:
                        for row_idx, row in df.iloc[idx: idx + 10].iterrows():
                            sum += row['dkx']
                    sum /= 10
                    dkx_sma[idx] = sum
                df['dkx_sma'] = dkx_sma.tolist()

                df = df.iloc[::-1]
                df.reset_index(drop=True, inplace=True)

                with open('data.txt', 'w') as f:
                    # check dkx crossing up dkx_ma
                    if df.iloc[-2]['dkx'] < df.iloc[-2]['dkx_sma'] and df.iloc[-1]['dkx'] > df.iloc[-1]['dkx_sma']:           
                        # check macd and signal
                        macd_df = ta.macd(df['收盘'], self.m_macd_config['fast'], self.m_macd_config['slow'], self.m_macd_config['signal'])
                        if macd_df.iloc[-1]['MACD_12_26_9'] > 0 and macd_df.iloc[-1]['MACD_12_26_9'] > macd_df.iloc[-1]['MACDs_12_26_9']:
                            logger.success('{0} Cross Up !'.format(symbol))
                            f.writelines('{0} Cross Up!'.format(symbol))
            except:
                logger.error('process {0} error !'.format(symbol)) 


if __name__ == '__main__':
    stock_bot = StockBot()
    stock_bot.get_symbols()
    stock_bot.dkx_cross_strategy(period='daily', start_date='20230121', end_date='20240221')



