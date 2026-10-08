import tushare as ts
from datetime import datetime, timedelta


analysis_trade_date = "20261008"
date_obj = datetime.strptime(analysis_trade_date, "%Y%m%d")
year_str = date_obj.year
analysis_trade_date_start = f"{year_str}0101" #组合当年的第一天
print(f"日期：{analysis_trade_date_start}-{analysis_trade_date}")

def getPro():
    return getPro_5000()

def getPro_self():
    token_self = "d6a7b03012743e8b035a4c37ec258b77fb5a65500c0629e0022cffde"
    pro = ts.pro_api(token_self)
    return pro

def getPro_5000():
    # 20261124
    token_5000 = "4dfe55ae66614ca943e09a6d82339eb65b77dcaf327841ba3d5c1574"
    pro = ts.pro_api(token_5000)

    return pro


def getPro_10000():
    # 20261201
    token_100000 = "gg99b01a244311f7742512463d9e1588954d174b86d695f8a9f1cc4b"
    pro = ts.pro_api(token_100000)
    return pro