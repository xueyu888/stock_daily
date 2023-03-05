import pandas as pd
import pymysql

# 连接MySQL数据库
host = 'localhost'
user = 'xue'
password = '123456'
database = 'stock'
port = 3306
conn = pymysql.connect(host=host, user=user, password=password, database=database, port=port)

# 从数据库中获取股票列表
cursor = conn.cursor()
sql = "SELECT DISTINCT ts_code FROM daily_price"
cursor.execute(sql)
result = cursor.fetchall()
stocks = [r[0] for r in result]

# 遍历每个股票
selected_stocks = []
for ts_code in stocks:
    # 从数据库中获取指定股票代码的历史K线数据
    sql = f"SELECT trade_date, open_price, close_price, high, low, vol FROM daily_price WHERE ts_code='{ts_code}' ORDER BY trade_date DESC LIMIT 20"
    df = pd.read_sql(sql, conn)

    # 计算振幅
    amplitude = (df['high'].max() - df['low'].min()) / df['low'].min()

    # 满足条件1：最近20个交易日振幅大于25%
    if amplitude <= 0.25:
        continue

    # 满足条件2：最近20个交易日创下了100日新高
    # highest = df['high'].rolling(100).max()
    # if not (df['high'] == highest).any():
    #     continue

    # 满足条件3：最近20个交易日至少有一天的涨幅大于9.5%
    # close_pct_change = df['close_price'].pct_change()
    # if not (close_pct_change > 0.095).any():
    #     continue

    # 满足条件4：上涨时收盘价在五日线之上，下跌反之
    # df['ma5'] = df['close_price'].rolling(5).mean()
    # ma5 = df['ma5'].values
    # close_price = df['close_price'].values
    # if ma5[-1] > ma5[0]:
    #     if not (close_price > ma5).all():
    #         continue
    # else:
    #     if not (close_price < ma5).all():
    #         continue

    # 满足条件5：当天站上五日线，涨幅在3% - 5%之间
    # today_close = close_price[-1]
    # today_ma5 = ma5[-1]
    # if today_close < today_ma5:
    #     continue
    # yesterday_close = close_price[-2]
    # yesterday_ma5 = ma5[-2]
    # if yesterday_close > yesterday_ma5:
    #     continue
    # today_pct_change = (today_close - yesterday_close) / yesterday_close
    # if today_pct_change < 0.03 or today_pct_change > 0.05:
    #     continue

    # 将符合条件的股票代码加入选中列表
    selected_stocks.append(ts_code)

# 输出符合条件的股票代码
print(selected_stocks)


#关闭数据库连接
conn.close()
