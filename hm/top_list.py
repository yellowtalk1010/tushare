# 导入tushare
from time import sleep

import tushare as ts
import pandas as pd
from fontTools.misc.cython import returns

import ZukTuShare
from datetime import datetime, timedelta

# 初始化pro接口
# 初始化pro接口
pro = ZukTuShare.getPro_10000()

# 拉取数据
def top_list(trade_date):
    df = pro.top_list(**{
        "trade_date": trade_date,
        "ts_code": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "trade_date",
        "ts_code",
        "name",
        "close",
        "pct_change",
        "turnover_rate",
        "amount",
        "l_sell",
        "l_buy",
        "l_amount",
        "net_amount",
        "net_rate",
        "amount_rate",
        "float_values",
        "reason"
    ])
    print(df)
    return df



def do_top_list():
    trade_date = ZukTuShare.analysis_trade_date
    date_obj = datetime.strptime(trade_date, "%Y%m%d")

    for i in range(0, 60):
        # 输出前7天（含当天）
        day = date_obj - timedelta(days=i)
        year_str = day.year
        month_str = day.month
        day_str = day.day
        print(f"year:{year_str}, month:{month_str}, day:{day_str}")

        pre_trade_day = day.strftime("%Y%m%d")
        # print(pre_trade_day)

        df = top_list(pre_trade_day)
        if len(df) > 0:
            path = f"top_list/{year_str}/{pre_trade_day}_top_list.csv"
            print(f"保存到：{path}")
            df.to_csv(path, encoding="utf-8", index=False)
        else:
            print(f"数量为:{len(df)}，不保存")

        sleep(2)


# 股票数据/打板专题数据/龙虎榜每日统计单
if __name__ == '__main__':
    print("股票数据/打板专题数据/龙虎榜每日统计单（这个数据不知道怎么用）")
    do_top_list()
    print("完成")


