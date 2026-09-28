import os

import tushare as ts
import pandas as pd
import time
from pathlib import Path

import ZukTuShare

daily_path = "daily"

def down_load_daily(ts_code, trade_date, start_date, end_date):
    time.sleep(2)
    # 导入tushare
    # 初始化pro接口
    pro = ZukTuShare.getPro()

    # 拉取数据
    df = pro.daily(**{
        "ts_code": ts_code,
        "trade_date": trade_date,
        "start_date": start_date,
        "end_date": end_date,
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount"
    ])
    # print(df)

    print(f"下载数据记录数:{len(df)}")

    stocks = pd.read_csv(
        "all_stocks.csv",
        dtype={
            "ts_code": str,
            "name": str
        }
    )
    stocks["ts_code"] = stocks["ts_code"].fillna("").astype(str).str.strip()
    stocks["name"] = stocks["name"].fillna("").astype(str).str.strip()

    name_map = dict(zip(stocks["ts_code"], stocks["name"]))

    df["ts_code"] = df["ts_code"].fillna("").astype(str).str.strip()
    df["name"] = df["ts_code"].map(name_map).fillna("")

    name_col = df.pop("name")
    ts_code_index = df.columns.get_loc("ts_code")
    df.insert(ts_code_index + 1, "name", name_col)

    print(f"合并记录数:{len(df)}")

    return df



# def download_all():
#     all_stocks = pd.read_csv("all_stocks.csv")
#     print(all_stocks.__len__())
#     try:
#         for index, row in all_stocks.iterrows():
#             ts_code = row["ts_code"]
#             name = row["name"]
#             print(f"{index}, {ts_code}, {name}")
#
#             new_code = ts_code.replace(".", "_")
#             path = f"{daily_path}/{new_code}/"
#
#             if os.path.exists(path) is False:
#                 os.makedirs(path)
#
#             file = path + "/202511.csv"
#             try:
#                 if os.path.exists(file) is False:
#                     df = daily(ts_code)
#                     df.to_csv(file, index=False)
#                 print(f"{file}，成功")
#             except:
#                 os.remove(file)
#                 print(f"{file}，失败")
#     except Exception as e:
#         print(e)

def download_one(ts_code_path, start_date, end_date):
    ts_code = ts_code_path.replace("_", ".")
    df = down_load_daily(ts_code, "",start_date, end_date)
    path = f"{daily_path}/{ts_code_path}/2026.csv"
    df.to_csv(path, index=False)
    print(f"完成。{path}，{len(df)}")

def download_trade_date(trade_date):
    if len(trade_date) != 8:
        print(f"交易日期格式异常{trade_date}，日期格式：yyyyMMdd")
        return

    year = trade_date[0:4] #取yyyyMMdd中的yyyy部分
    path = f"{daily_path}/{trade_date}.csv"
    if os.path.exists(path) is False:
        trade_date_df = down_load_daily("",
                                        trade_date,
                                        "",
                                        "")
        if len(trade_date_df) == 0:
            print(f"下载的数据长度为:{len(trade_date_df)}，下载失败，检查日期是否格式错误，或非交易日")
            return
        else:
            # 保存到文件中
            trade_date_df.to_csv(path, index=False)
            print(f"{path}，路径不存在，下载完成")
    else:
        print(f"{path}，路径存在，已下载完成")

    # 读取已经下载的文件
    flat_df = pd.read_csv(path,
                          dtype={
                              "trade_date": str
                          })
    print(f"总数：{len(flat_df)}")

    # 均衡写入对应的数据年度数据中去
    merge_num = 0
    for index, flat_row in flat_df.iterrows():
        try:
            ts_code = flat_row["ts_code"] # tushare股票代码
            name = flat_row["name"] # tushare股票名称
            ts_code_path = ts_code.replace(".", "_") # 将股票中的“.”转成“_”
            year_file = f"{daily_path}/{ts_code_path}/{year}.csv"
            Path(year_file).parent.mkdir(parents=True, exist_ok=True)

            if os.path.exists(year_file) is False:
                print(f"{year_file}，文件不存在")
                df = down_load_daily(ts_code,
                                     "",
                                     ZukTuShare.analysis_trade_date_start, #今年的第一天
                                     trade_date)
                df.to_csv(year_file)
                continue

            # print(f"读取{year_file}")
            year_df = pd.read_csv(year_file,
                                  dtype={
                                      "trade_date": str
                                  })

            # 判断交易日期是否已经加载
            exist_trade_date_rows = year_df[year_df["trade_date"] == trade_date]

            if len(exist_trade_date_rows) > 0:
                print(f"{ts_code}, {name}, {trade_date}，记录已经存在")
                continue
            else:
                print(f"{ts_code}, {name}, {trade_date}，记录不存在")
                merge = pd.concat([pd.DataFrame([flat_row.to_dict()]), year_df], ignore_index=True)
                merge = merge.loc[:, ~merge.columns.str.contains('^Unnamed')]
                # merge根据trade_date从大到小排序
                merge = merge.sort_values(by="trade_date", ascending=False)
                merge.to_csv(year_file, index=False)
                print(f"{ts_code}, {trade_date}，合并成功，{year_file}")
                merge_num = merge_num + 1
        except Exception as e:
            print(f"错误，{e.__str__()}")

    print(f"合并完成，{merge_num}")

if __name__ == '__main__':

    # download_one("001388_SZ", "20260101", "20261231")
    download_trade_date(ZukTuShare.analysis_trade_date)
    print("完成")




