import polars as pl

from config import PROCESSED_ROOT

# 查看 stage3 sessionized 的 MBP-10 数据
stage3_path = PROCESSED_ROOT / 'stage3_sessionized' / 'mbp-10' / 'und' / '2026-05-12'
files = list(stage3_path.glob('*.parquet'))

if files:
    print(f"读取文件: {files[0]}")
    df = pl.read_parquet(files[0])

    # 查看一些样本数据
    print("\n--- 前5行样本数据 ---")
    print(df.select([
        'ts_recv_ns',
        'bid_px_00', 'bid_sz_00', 'bid_ct_00',
        'ask_px_00', 'ask_sz_00', 'ask_ct_00',
        'bid_px_09', 'bid_sz_09', 'bid_ct_09',
        'ask_px_09', 'ask_sz_09', 'ask_ct_09',
    ]).head(5))

    print("\n--- 数据类型对比 ---")
    # 提取几个层的类型
    for level in range(10):
        px_col = f'bid_px_{level:02d}'
        sz_col = f'bid_sz_{level:02d}'
        ct_col = f'bid_ct_{level:02d}'
        print(f"Level {level:02d}:")
        print(f"  {px_col:<12} {df.schema[px_col]}")
        print(f"  {sz_col:<12} {df.schema[sz_col]}")
        print(f"  {ct_col:<12} {df.schema[ct_col]}")
