import pandas as pd
import pymysql
from sqlalchemy import create_engine, text

host = 'localhost'
user = 'xue'
password = '123456'
database = 'stock'
port = 3306

# 连接 MySQL 数据库
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', pool_size=200)

# 从数据库中获取股票列表
sql = "SELECT DISTINCT ts_code FROM daily_price"
stocks = pd.DataFrame(engine.connect().execute(text(sql)), columns=['ts_code'])['ts_code'].tolist()

# 遍历每个股票
selected_stocks = []
for ts_code in stocks:
    # 从数据库中获取指定股票代码的历史K线数据
    sql = f"SELECT trade_date, open_price, close_price, high, low, vol FROM daily_price WHERE ts_code='{ts_code}' ORDER BY trade_date DESC LIMIT 20"
    df = pd.DataFrame(engine.connect().execute(text(sql)), columns=['trade_date', 'open_price', 'close_price', 'high', 'low', 'vol'])

    # 计算最大涨幅
    max_gain = (df['close_price'].max() - df[df['close_price'].idxmin():]['close_price'].min()) / df[df['close_price'].idxmin():]['close_price'].min()

    # 满足条件1：最近20个交易日内的最大涨幅大于25%
    if max_gain <= 0.25:
        continue

    # 将符合条件的股票代码加入选中列表
    selected_stocks.append(ts_code)

# 输出符合条件的股票代码
print(selected_stocks)

# 关闭数据库连接
engine.dispose()
