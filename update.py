import os
import glob
import pandas as pd


def update_csv_files():
    stocks = pd.read_csv(
        "all_stocks.csv",
        dtype={
            "symbol": str,
            "ts_code": str,
            "name": str
        }
    )
    print(f"股票数量: {len(stocks)}")

    updated_file_count = 0
    skipped_dir_count = 0

    for index, row in stocks.iterrows():
        ts_code = str(row["ts_code"]).strip()
        stock_name = str(row["name"]).strip() if pd.notna(row["name"]) else ""


        # 000001.SZ -> 000001_SZ
        dir_name = ts_code.replace(".", "_")
        dir_name = f"./daily/{dir_name}"

        print(f"{index}: {ts_code} -> {dir_name}, name={stock_name}")

        new_stock_name = "" if "ST" in stock_name.upper() else stock_name
        # new_stock_name = stock_name

        # 以修改后的 ts_code 作为目录路径
        if not os.path.isdir(dir_name):
            skipped_dir_count += 1
            print(f"目录不存在，跳过: {dir_name}")
            continue

        csv_files = glob.glob(os.path.join(dir_name, "*.csv"))
        if not csv_files:
            print(f"目录下没有 csv 文件: {dir_name}")
            continue
        else:
            print(f"文件数:{len(csv_files)}")


        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file, dtype=str)

                # if "ts_code" not in df.columns:
                #     print(f"缺少 ts_code 列，跳过: {csv_file}")
                #     continue
                #
                # if "name" in df.columns:
                #     print(f"已存在 name 列，跳过: {csv_file}")
                #     continue
                #
                # ts_code_index = df.columns.get_loc("ts_code")
                # df.insert(ts_code_index + 1, "name", new_stock_name)

                df.to_csv(csv_file, index=False, encoding="utf-8")
                updated_file_count += 1
                print(f"已新增 name 列: {csv_file}")

            except Exception as e:
                print(f"处理失败: {csv_file}, error={e}")

    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    print(f"更新文件数: {updated_file_count}")
    print(f"不存在的目录数: {skipped_dir_count}")


if __name__ == "__main__":
    update_csv_files()