# 导入tushare
from time import sleep

import tushare as ts
import pandas as pd
import ZukTuShare
from datetime import datetime, timedelta

# 初始化pro接口
pro = ZukTuShare.getPro_10000()

# 拉取数据
def top_inst(trade_date):
    df = pro.top_inst(**{
        "trade_date": trade_date,
        "ts_code": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "trade_date",
        "ts_code",
        "exalter",
        "buy",
        "buy_rate",
        "sell",
        "sell_rate",
        "net_buy",
        "side",
        "reason"
    ])
    # print(df)
    print(len(df))
    return df


def do_top_inst():
    trade_date = ZukTuShare.analysis_trade_date
    date_obj = datetime.strptime(trade_date, "%Y%m%d")

    for i in range(0, 7):
        # 输出前7天（含当天）
        day = date_obj - timedelta(days=i)
        year_str = day.year
        month_str = day.month
        day_str = day.day
        print(f"year:{year_str}, month:{month_str}, day:{day_str}")

        pre_trade_day = day.strftime("%Y%m%d")
        # print(pre_trade_day)

        df = top_inst(pre_trade_day)
        if len(df) > 0:
            path = f"top_inst/{year_str}/{pre_trade_day}_top_inst.csv"
            print(f"保存到：{path}")
            df.to_csv(path, encoding="utf-8", index=False)
        else:
            print(f"数量为:{len(df)}，不保存")

        sleep(2)



# 股票数据/打板专题数据/龙虎榜机构交易单
if __name__ == '__main__':
    print("股票数据/打板专题数据/龙虎榜机构交易单")
    do_top_inst()
    print("完成")