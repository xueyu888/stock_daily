import pymysql
import tushare as ts
from datetime import datetime, timedelta
from requests.exceptions import Timeout

# 连接MySQL数据库
host = 'localhost'
user = 'xue'
password = '123456'
database = 'stock'
port = 3306
conn = pymysql.connect(host=host, user=user, password=password, database=database, port=port)

# 创建 daily_price 表
cursor = conn.cursor()
sql = '''CREATE TABLE IF NOT EXISTS daily_price (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    open_price FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    close_price FLOAT NOT NULL,
    pre_close FLOAT NOT NULL,
    change_price FLOAT NOT NULL,
    pct_chg FLOAT NOT NULL,
    vol FLOAT NOT NULL,
    amount FLOAT NOT NULL,
    PRIMARY KEY(ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
'''
cursor.execute(sql)
conn.commit()

# 获取股票列表
pro = ts.pro_api('f1ea73e8c3bccec030b799926bf0d411c479432c081e03b3e54fcbd3')
stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')

# 获取最近十年的交易日历
today = datetime.now().strftime('%Y%m%d')
start_date = (datetime.now() - timedelta(days=3650)).strftime('%Y%m%d')
trade_cal = pro.trade_cal(start_date=start_date, end_date=today, is_open='1')
trade_days = trade_cal['cal_date'].tolist()

# 循环获取每个股票的历史数据，并将其存储到MySQL数据库中
for index, row in stocks.iterrows():
    ts_code = row['ts_code']
    print(f'Processing {ts_code}')

    # 获取每个股票的历史数据，最多尝试5次
    for attempt in range(5):
        try:
            df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=today, timeout=5)
            break
        except Timeout:
            print(f'Request timed out, retrying in 3 seconds... (attempt {attempt+1}/5)')
            time.sleep(3)
    else:
        print(f'Could not get data for {ts_code} after 5 attempts, skipping...')
        continue

    # 将数据写入MySQL数据库
    for index, row in df.iterrows():
        trade_date = row['trade_date']
        if trade_date not in trade_days:
            continue
        open_price = row['open']
        close_price = row['close']
        high_price = row['high']
        low_price = row['low']
        pre_close_price = row['pre_close']
        change_price = row['change']
        pct_chg = row['pct_chg']
        volume = row['vol']
        amount = row['amount']
        cursor = conn.cursor()
        sql = f"INSERT IGNORE  INTO daily_price (ts_code, trade_date, open_price, high, low, close_price, pre_close, change_price, pct_chg, vol, amount) VALUES ('{ts_code}', '{trade_date}', {open_price}, {high_price}, {low_price}, {close_price}, {pre_close_price}, {change_price}, {pct_chg}, {volume}, {amount})"
        cursor.execute(sql)
        conn.commit()
    cursor.close()
conn.close()
