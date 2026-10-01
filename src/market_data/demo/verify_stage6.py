import polars as pl

from config import STAGE6_DIR

# 随机挑选一个日期验证
test_date = "2026-05-15"
print(f"--- Verifying Stage6 output for {test_date} ---")

# 检查 und
path_und = STAGE6_DIR / "aligned" / "und" / test_date / "aligned_trade_book.parquet"
if path_und.exists():
    df_und = pl.read_parquet(path_und)
    print(f"\n--- und data ---")
    print(f"Total rows: {len(df_und)}")
    print(f"Columns: {df_und.columns}")
    print("\nSample price data (first 5 rows):")
    print(df_und.select(["price", "price_f", "bid_f", "ask_f"]).head())

# 检查 opt
path_opt = STAGE6_DIR / "aligned" / "opt" / test_date / "aligned_trade_book.parquet"
if path_opt.exists():
    df_opt = pl.read_parquet(path_opt)
    print(f"\n--- opt data ---")
    print(f"Total rows: {len(df_opt)}")
    print("\nSample price data (first 5 rows):")
    print(df_opt.select(["price", "price_f", "bid_f", "ask_f"]).head())
