import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
import time
import concurrent.futures

host = 'localhost'
user = 'xue'
password = '123456'
database = 'stock'
port = 3306

def process_stock(ts_code):
    # 连接 MySQL 数据库
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', pool_size=2000)

    # 从数据库中获取指定股票代码的历史K线数据
    sql = f"SELECT trade_date, open_price, close_price, high, low, vol FROM daily_price WHERE ts_code='{ts_code}' ORDER BY trade_date DESC LIMIT 20"
    df = pd.DataFrame(engine.connect().execute(text(sql)), columns=['trade_date', 'open_price', 'close_price', 'high', 'low', 'vol'])

    # 计算最大涨幅
    max_gain = (df['close_price'].max() - df[df['close_price'].idxmin():]['close_price'].min()) / df[df['close_price'].idxmin():]['close_price'].min()

    # 满足条件1：最近20个交易日内的最大涨幅大于25%
    if max_gain <= 0.25:
        # 关闭数据库连接
        engine.dispose()
        return None

    # 关闭数据库连接
    engine.dispose()

    return ts_code

if __name__ == '__main__':
    start = time.time()

    # 连接 MySQL 数据库
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', pool_size=2000)

    # 从数据库中获取股票列表
    sql = "SELECT DISTINCT ts_code FROM daily_price"
    stocks = pd.DataFrame(engine.connect().execute(text(sql)), columns=['ts_code'])['ts_code'].tolist()

    # 使用多线程处理股票数据
    selected_stocks = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [executor.submit(process_stock, ts_code) for ts_code in stocks]
        for future in concurrent.futures.as_completed(results):
            result = future.result()
            if result:
                selected_stocks.append(result)

    # 输出符合条件的股票代码

    # 关闭数据库连接
    engine.dispose()
    print(selected_stocks)
    end = time.time()
    print(f"Elapsed time: {end - start} seconds")
