import polars as pl

from config import PROCESSED_ROOT

# 查看 stage3 sessionized 的 MBP-10 数据
stage3_path = PROCESSED_ROOT / 'stage3_sessionized' / 'mbp-10' / 'und' / '2026-05-12'
files = list(stage3_path.glob('*.parquet'))

if files:
    print(f"读取文件: {files[0]}")
    df = pl.read_parquet(files[0])

    # 查看哪些地方有 NaN 价格
    print("\n--- 检查 NaN 出现的情况 ---")

    # 检查第9层的价格，通常可能更容易出现空档位
    bid9_null = df.select(pl.col('bid_px_09').is_null()).sum()
    ask9_null = df.select(pl.col('ask_px_09').is_null()).sum()

    print(f"bid_px_09 有 NaN 的行数: {bid9_null}")
    print(f"ask_px_09 有 NaN 的行数: {ask9_null}")

    # 查看一些有 NaN 的样本行
    print("\n--- 查看有 NaN 的样本行 ---")
    null_samples = df.filter(pl.col('bid_px_09').is_null()).head(3)
    print(null_samples.select([
        'ts_recv_ns',
        'bid_px_00', 'bid_sz_00', 'bid_ct_00',
        'bid_px_09', 'bid_sz_09', 'bid_ct_09',
        'ask_px_09', 'ask_sz_09', 'ask_ct_09',
    ]))
