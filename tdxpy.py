import pandas as pd
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

# 从数据库中获取股票列表
sql = "SELECT DISTINCT ts_code FROM daily_price"
stocks = pd.DataFrame(engine.connect().execute(text(sql)), columns=['ts_code'])['ts_code'].tolist()

# 遍历每个股票
selected_stocks = []
for ts_code in stocks:
    # 从数据库中获取指定股票代码的历史K线数据
    sql = f"SELECT trade_date, open_price, close_price, high, low, vol FROM daily_price WHERE ts_code='{ts_code}' ORDER BY trade_date DESC LIMIT 50"
    df = pd.DataFrame(engine.connect().execute(text(sql)), columns=['trade_date', 'open_price', 'close_price', 'high', 'low', 'vol'])

    # 计算收盘价是否在五日均线之上
    df['ma5'] = df['close_price'].rolling(5).mean()
    above_ma5 = df['close_price'] > df['ma5']

    # 计算5日涨幅是否大于25%
    pct_change = df['close_price'].pct_change(5)
    up_trend = (pct_change > 0.25) & above_ma5.shift(-4).fillna(False)

    # 计算第二波下跌是否符合条件
    below_ma5 = df['close_price'] < df['ma5']
    down_trend = below_ma5 & (below_ma5.shift(1) | below_ma5.shift(2) | below_ma5.shift(3))
    continuous_drop_days = down_trend.rolling(7, min_periods=4).sum()

    # 计算上穿五日线是否符合条件
    above_ma5_again = df['close_price'] > df['ma5']
    cross_above_ma5 = above_ma5_again & (above_ma5_again.shift(1) | above_ma5_again.shift(2) | above_ma5_again.shift(3) | above_ma5_again.shift(4))
    pct_change_again = df['close_price'].pct_change().fillna(0)
    close_gain = pct_change_again[above_ma5_again & cross_above_ma5]
    
    # 满足条件1和2
    if up_trend.any() and (continuous_drop_days >= 4).any():
        # 找到满足条件1和2的第一个位置
        start = up_trend[up_trend].index[-1]

        # 判断是否满足条件3
        for end in continuous_drop_days[continuous_drop_days >= 4].index:
            if end < start:
                continue
            if (close_gain.loc[start:end] >= 0.02).any() and (close_gain.loc[start:end] < 0.06).all():
                selected_stocks.append(ts_code)
                break

# 输出符合条件的股票代码
print(selected_stocks)

# 关闭数据库连接
engine.dispose()
