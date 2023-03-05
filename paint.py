import pymysql
import pandas as pd
import mplfinance as mpf

# 连接MySQL数据库
host = 'localhost'
user = 'xue'
password = '123456'
database = 'stock'
port = 3306
conn = pymysql.connect(host=host, user=user, password=password, database=database, port=port)

# 从数据库中获取指定股票代码的历史K线数据
ts_code = '000001.SZ'
cursor = conn.cursor()
sql = f"SELECT trade_date, open_price, close_price, high, low, vol FROM daily_price WHERE ts_code='{ts_code}' ORDER BY trade_date ASC"
cursor.execute(sql)
result = cursor.fetchall()

# 将数据转换成DataFrame格式
df = pd.DataFrame(result, columns=['trade_date', 'Open', 'Close', 'High', 'Low', 'Volume'])
df['trade_date'] = pd.to_datetime(df['trade_date'])
df.set_index('trade_date', inplace=True)

# 创建20日和60日均线附加图表
ma20 = df['Close'].rolling(window=20).mean()
ma60 = df['Close'].rolling(window=60).mean()
apds = [mpf.make_addplot(ma20, color='b'), mpf.make_addplot(ma60, color='r')]

# 绘制K线图和附加图表
mpf.plot(df, type='candle', volume=True, show_nontrading=False, addplot=apds, ylabel='Price')

# 调整y轴范围以查看日K具体形态
ylim_bottom, ylim_top = plt.ylim()
plt.ylim(ylim_bottom - (ylim_top - ylim_bottom) * 0.1, ylim_top)

# 显示K线图
mpf.show()
