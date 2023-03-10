抱歉，我看到我之前的代码有些问题，需要进行修改。

首先，我建议将selected_stocks列表中的字典改为元组，这样可以避免后面误修改。修改后的selected_stocks定义如下：

css
Copy code
selected_stocks = []
其次，我建议在状态为buy的情况下将股票代码和购买日期存储到selected_stocks列表中。修改后的代码如下：

lua
Copy code
elif status == 'buy':
    selected_stocks.append((ts_code, row['trade_date'], row['close_price']))
    status = 'sold'
最后，计算收益率的函数calculate_return也需要进行修改，修改后的代码如下：

sql
Copy code
def calculate_return(row, selected_stocks):
    for stock in selected_stocks:
        if stock[0] == row['ts_code']:
            sell_date = row['trade_date']
            sell_price = row['close_price']
            buy_date = stock[1]
            buy_price = stock[2]
            if sell_date != buy_date:
                return_rate = (sell_price - buy_price) / buy_price
                selected_stocks.remove(stock)
                return return_rate
    return None
然后，在循环遍历每个股票的结束后，我们可以将selected_stocks中剩余未卖出的股票按当日收盘价卖出。修改后的代码如下：

python
Copy code
for stock in selected_stocks:
    ts_code = stock[0]
    buy_date = stock[1]
    buy_price = stock[2]
    # 获取股票在当日的收盘价
    sql = f"SELECT close_price FROM daily_price WHERE ts_code='{ts_code}' AND trade_date='{today_date}'"
    sell_price = engine.connect().execute(text(sql)).fetchone()[0]
    return_rate = (sell_price - buy_price) / buy_price
    total_return += return_rate
    count += 1
    print(f"股票代码：{ts_code}，购买日期：{buy_date}，购买价：{buy_price}，卖出日期：{today_date}，卖出价：{sell_price}，收益率：{return_rate:.2%}")

# 计算所有股票的平均收益率
average_return = total_return / count if count > 0 else 0
print(f"所有股票平均收益率：{average_return:.2%}")
希望这次修改能够解决问题。

0.1355855856
0.1081947506
0.0398560384
0.0249221184
0.0167837627
0.011820331
0.0095162569
-0.0027258826
-0.0027662517
-0.0069387755
-0.013215859
-0.0144927536
-0.0197916667
-0.0267379679
-0.0389830508
-0.0711316397