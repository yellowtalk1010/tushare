# 导入tushare
import tushare as ts
from tornado.gen import sleep

import ZukTuShare
import pandas as pd
# 导入tushare
from datetime import datetime, timedelta

# 初始化pro接口
pro = ZukTuShare.getPro_10000()

# 下载东方财富资金流向
def download_moneyflow_dc(trade_date):
    # 拉取数据
    df = pro.moneyflow_dc(**{
        "ts_code": "",
        "trade_date": trade_date,
        "start_date": "",
        "end_date": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "trade_date",
        "ts_code",
        "name",
        "pct_change",
        "close",
        "net_amount",
        "net_amount_rate",
        "buy_elg_amount",
        "buy_elg_amount_rate",
        "buy_lg_amount",
        "buy_lg_amount_rate",
        "buy_md_amount",
        "buy_md_amount_rate",
        "buy_sm_amount",
        "buy_sm_amount_rate"
    ])
    # print(df)
    # print(len(df))
    return df


def do_moneyflow():
    trade_date = ZukTuShare.analysis_trade_date
    date_obj = datetime.strptime(trade_date, "%Y%m%d")

    for i in range(0, 8):
        # 输出前7天（含当天）
        day = date_obj - timedelta(days=i)
        pre_trade_day = day.strftime("%Y%m%d")
        print(pre_trade_day)

        df = download_moneyflow_dc(pre_trade_day)
        if len(df) > 0:
            path = f"data/moneyflow_dc/{pre_trade_day}.csv"
            print(f"保存到：{path}")
            df.to_csv(f"{path}", encoding="utf-8", index=False)
        else:
            print(f"数量为:{len(df)}，不保存")

        sleep(500)

if __name__ == '__main__':
    str = "拉去资金流向"
    print(f"开始{str}")
    do_moneyflow()
    print(f"完成{str}")

