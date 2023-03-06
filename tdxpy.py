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
    sql = f"SELECT trade_date, open_price, close_price, high, low, vol FROM daily_price WHERE ts_code='{ts_code}' ORDER BY trade_date ASC LIMIT 50"
    df = pd.DataFrame(engine.connect().execute(text(sql)), columns=['trade_date', 'open_price', 'close_price', 'high', 'low', 'vol'])

    # 计算MA5
    if len(df) >= 5:
        ma5 = df['close_price'].rolling(5).mean().shift(1)
        df['MA5'] = ma5

        # 状态机
        status = 'start'
        last_rise = -5
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
                        status = 'start'
                        selected_stocks.append(ts_code)
            elif status in ['start', 'hold'] and row['close_price'] > row['MA5']:
                if last_cross is None or last_cross < i - 5:
                    if row['close_price'] > df.loc[i-1, 'MA5']:
                        status = 'buy'
                        last_cross = i

print(selected_stocks)
