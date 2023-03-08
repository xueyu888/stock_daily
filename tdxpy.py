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
selected_stocks = [{'date': '', 'stock': ''}]
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
                            if i+5 >= len(df):
                                break  # 数据不足6天，退出循环
                            temp_df = df.loc[i:i+5]  # 取连续的6天数据
                            gain = (temp_df['close_price'].iloc[5] - temp_df['close_price'].iloc[0]) / temp_df['close_price'].iloc[0]
                            if gain > 0.25:  # 涨幅大于25%
                                status = 'rising'
                                last_rise = i
            elif status == 'rising':
                if row['close_price'] < row['MA5']:
                    if consecutive_below_MA5 == 1:
                        status = 'waiting'
                else:
                    consecutive_below_MA5 = 0
                consecutive_below_MA5 += 1
            elif status == 'waiting':
                consecutive_below_MA5 = 0
                if row['close_price'] < row['MA5']:
                    if df.loc[i-4:i, 'close_price'].lt(df.loc[i-4:i, 'MA5']).all():
                        status = 'falling'
            elif status == 'falling' and row['close_price'] > row['MA5']:
                    if row['close_price'] > df.loc[i-1, 'MA5']:
                        status = 'buy'
                        selected_stocks.append({'date': row['trade_date'], 'stock': ts_code})

print(selected_stocks)
end = time.time()
print(end -start)
