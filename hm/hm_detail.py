from time import sleep

# 导入tushare
import tushare as ts
import ZukTuShare
import pandas as pd
from datetime import datetime, timedelta

# 初始化pro接口
pro = ZukTuShare.getPro_10000()

# 游资交易每日明细
def download_hm_detail(trade_date):
    # 拉取数据
    df = pro.hm_detail(**{
        "trade_date": trade_date,
        "ts_code": "",
        "hm_name": "",
        "start_date": "",
        "end_date": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "trade_date",
        "ts_code",
        "ts_name",
        "buy_amount",
        "sell_amount",
        "net_amount",
        "hm_name",
        "hm_orgs",
        "tag"
    ])
    # print(df)
    print(len(df))
    return df

# 股票数据/打板专题数据/龙游资交易每日明细
if __name__ == '__main__':
    print("游资交易每日明细")
    trade_date = ZukTuShare.analysis_trade_date
    date_obj = datetime.strptime(trade_date, "%Y%m%d")

    for i in range(0, 8):
        # 输出前7天（含当天）
        day = date_obj - timedelta(days=i)
        pre_trade_day = day.strftime("%Y%m%d")
        # print(pre_trade_day)

        df = download_hm_detail(pre_trade_day)

        if len(df) > 0:
            path = f"hm_detail/hm_detail-{pre_trade_day}.csv"
            print(path)
            df.to_csv(f"{path}", encoding="utf-8", index=False)
        else:
            print(f"数量为:{len(df)}，不保存")

        sleep(2)

    print("完成")
