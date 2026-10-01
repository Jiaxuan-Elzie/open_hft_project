import polars as pl

from config import STAGE6_DIR

# 查看 Stage6 的对齐后的数据
stage6_path = STAGE6_DIR / 'aligned' / 'und' / '2026-05-12'
files = list(stage6_path.glob('*.parquet'))

if files:
    print(f"读取 Stage6 输出文件: {files[0]}")
    df = pl.read_parquet(files[0])

    # 查看关键价格列
    print("\n--- 前5行价格数据对比 ---")
    key_cols = [
        'price', 'best_bid_px', 'best_ask_px',  # 原始输入列
        'price_f', 'bid_f', 'ask_f'  # Stage6 生成的列
    ]
    print(df.select(key_cols).head(10))
