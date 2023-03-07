import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine, text
import time

host = 'localhost'
user = 'xue'
password = '123456'
database = 'stock'
port = 3306

start = time.time()

# 连接 MySQL 数据库
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', pool_size=2000)

# 从数据库中获取创业板股票列表
sql = "SELECT DISTINCT ts_code FROM daily_price WHERE ts_code LIKE '300%'"
stocks = pd.DataFrame(engine.connect().execute(text(sql)), columns=['ts_code'])['ts_code'].tolist()

# 遍历每个股票
selected_stocks = []
for ts_code in stocks:
    # 从数据库中获取指定股票代码的历史K线数据
    sql = f"SELECT trade_date, open_price, close_price, high, low, vol FROM daily_price WHERE ts_code='{ts_code}' ORDER BY trade_date DESC LIMIT 50"
    df = pd.DataFrame(engine.connect().execute(text(sql)), columns=['trade_date', 'open_price', 'close_price', 'high', 'low', 'vol'])
    df = df.sort_values(by='trade_date', ascending=True).reset_index(drop=True)

    # 计算MA5
    if len(df) >= 5:
        ma5 = df['close_price'].rolling(5).mean()
        df['MA5'] = ma5

             # 状态机
        status = 'start'
        last_rise = -5
        consecutive_below_MA5 = 0
        for i, row in df.iterrows():
            if status == 'start':
                if row['close_price'] > row['MA5']:
                    temp_df = df.iloc[i-4:i+1]  # 取连续的5天数据
                    gain = temp_df['close_price'].diff().iloc[1:].sum() / temp_df['close_price'].iloc[4]  # 使用价差法计算涨幅
                    if gain > 0.25:  # 涨幅大于25%
                        status = 'rising'
                        last_rise = i
            elif status == 'rising':
                if row['close_price'] < row['MA5']:
                    temp_df = df.iloc[last_rise:i+1]  # 取连续的N天数据
                    gain = temp_df['close_price'].diff().iloc[1:].sum() / temp_df['close_price'].iloc[0]  # 使用价差法计算涨幅
                    if gain < -0.1:  # 跌幅大于10%
                        status = 'falling'
            elif status == 'falling':
                if row['close_price'] < row['MA5']:
                    consecutive_below_MA5 += 1
                    if consecutive_below_MA5 >= 4 and consecutive_below_MA5 <= 7 and all(df.iloc[i-consecutive_below_MA5+1:i+1]['close_price'] < df.iloc[i-consecutive_below_MA5+1:i+1]['MA5']):
                        status = 'waiting'
            elif status == 'waiting' and row['close_price'] > row['MA5']:
                    if row['close_price'] > df.loc[i-1, 'MA5']:
                        status = 'buy'
                        selected_stocks.append(ts_code)


print(selected_stocks)
