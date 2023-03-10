import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine, text
import time
from sqlalchemy.pool import QueuePool
import csv

# 创建连接池
pool = QueuePool(
    creator=lambda: pymysql.connect(
        host='localhost',
        user='xue',
        password='123456',
        database='stock',
        port=3306
    ),
    max_overflow=200,  # 最大溢出连接数，即连接池中连接数最多可以增加到的数量
    pool_size=5000,  # 连接池中连接的数量
    timeout=30,  # 获取连接的超时时间，单位秒
)

# 创建数据库连接
engine = create_engine('mysql+pymysql://', pool=pool)


start = time.time()

# 连接 MySQL 数据库
#engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', pool_size=40)

# 从数据库中获取创业板股票列表
sql = "SELECT DISTINCT ts_code FROM daily_price "
stocks = pd.DataFrame(engine.connect().execute(text(sql)), columns=['ts_code'])['ts_code'].tolist()

# 遍历每个股票
selected_stocks = [{'date': '', 'stock': ''}]
returns = [] # 创建一个空列表用于存储收益率
for ts_code in stocks:
    # 从数据库中获取指定股票代码的历史K线数据
    sql = f"SELECT trade_date, ts_code, open_price, close_price, high, low, vol FROM daily_price WHERE ts_code='{ts_code}' ORDER BY trade_date DESC LIMIT 50"
    df = pd.DataFrame(engine.connect().execute(text(sql)), columns=['trade_date','ts_code', 'open_price', 'close_price', 'high', 'low', 'vol'])
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
                        if i+4 >= len(df):
                            break # 数据不足五天，退出循环
                        next_five_days_df = df.loc[i:i+4] # 取接下来的五天数据，包括首次跌破的那一天
                        if next_five_days_df['close_price'].lt(next_five_days_df['MA5']).all(): # 如果接下来的五天收盘价都在五日线之下
                            status = 'falling'
                        else: # 否则回到start状态，重新寻找涨幅大于25%的情况
                            status = 'start'

            elif status == 'falling' and row['close_price'] > row['MA5']:
                    if row['close_price'] > df.loc[i-1, 'MA5']:
                        status = 'buy'
                        selected_stocks.append({'date': row['trade_date'], 'stock': ts_code}) # 添加买入信号到列表

                        # 遍历每个买入信号，计算收益率并添加到列表中                        
                        for signal in selected_stocks:
                            date_buy = signal['date']
                            stock_buy = signal['stock']
                            buy_row = df.loc[(df.ts_code == stock_buy) & (df.trade_date == date_buy)]
                            
                            if buy_row.empty:
                                continue
                                
                            buy_price = float(buy_row['close_price']) # 获取买入价格
                            
                            date_sell_index= int(buy_row.index.values) + 1 
                            sell_row= df.iloc[date_sell_index]
                            
                            sell_stock= sell_row.ts_code 
                            sell_date= sell_row.trade_date 
                            sell_close= float(sell_row.close_price)
                            
                            return_rate= (sell_close - buy_price )/buy_price 
                            
                            returns.append({'date': row['trade_date'], 'stock': ts_code, 'return_rate':return_rate})


# 将数据写入CSV文件
with open('returns.csv', mode='w', newline='') as csv_file:
    fieldnames = ['date', 'stock', 'return_rate']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for item in returns:
        writer.writerow({'date': item['date'], 'stock': item['stock'], 'return_rate': "{:.2%}".format(item['return_rate'])})

# 计算平均收益率并打印结果    
average_return_rate= np.mean(returns.returns)
                        
print(selected_stocks)
end = time.time()
print(end -start)
