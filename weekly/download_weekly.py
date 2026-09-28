import os

import tushare as ts
import pandas as pd
import time
from pathlib import Path

import ZukTuShare

# 初始化pro接口
pro = ZukTuShare.getPro_10000()

def downloadWeekly(trade_date):
    # 拉取数据
    df = pro.weekly(**{
        "ts_code": "",
        "trade_date": "20260403",
        "start_date": "",
        "end_date": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "close",
        "open",
        "high",
        "low",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount"
    ])
    print(df)
    print(len(df))
    return df


if __name__ == '__main__':
    print("weekly")
    weekly_df = downloadWeekly(20260101)
    weekly_df.to_csv("./data/2026_weekly.csv", index=False)