import logging
import polars as pl
from pathlib import Path

from config import STAGE3_DIR, STAGE4_DIR, get_processed_path, PRICE_UNIT

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

F_BAD_TS_RECV = 1 << 4

def clean_trades(trade_date: str, asset_type: str) -> None:
    """Stage 4: 清洗 trades 并分层"""
    in_dir = get_processed_path(STAGE3_DIR, asset_type, 'trades', trade_date)
    if not in_dir.exists():
        return

    logger.info(f"Cleaning Trades for {trade_date} - {asset_type}")
    lf = pl.scan_parquet(in_dir / "*.parquet")

    # 根据 MD 文档：
    # 1. 仅 Trade，剔除 F
    # 2. 价格、Size > 0
    # 3. 标记侧边 (side_known)
    # 4. auction 等异常继续留存只是打标并分 quality_bucket

    dirty_flags = (pl.col("flags") & F_BAD_TS_RECV) != 0
    is_non_trade = pl.col("action") != "T"
    has_invalid_price = pl.col("price") <= 0
    has_invalid_size = pl.col("size") <= 0
    has_unknown_side = ~pl.col("side").is_in(["B", "A"])
    has_negative_latency = pl.col("recv_event_latency_ns") < 0
    invalid_trade = is_non_trade | has_invalid_price | has_invalid_size

    cleaned = (
        lf
        .with_columns([
            pl.when(pl.col("side").is_in(["B", "A"])).then(pl.col("side")).otherwise(pl.lit("N")).alias("trade_side"),
            ((pl.col("price") / PRICE_UNIT) * pl.col("size")).alias("notional"),
            is_non_trade.alias("is_non_trade"),
            has_invalid_price.alias("has_invalid_price"),
            has_invalid_size.alias("has_invalid_size"),
            has_unknown_side.alias("has_unknown_side"),
            has_negative_latency.alias("has_negative_latency"),
            dirty_flags.alias("has_bad_ts_recv"),

            pl.when(invalid_trade | dirty_flags).then(pl.lit(False)).otherwise(pl.lit(True)).alias("is_valid_trade"),

            # 分层
            pl.when(invalid_trade).then(pl.lit("drop"))
              .when(dirty_flags | has_unknown_side | has_negative_latency).then(pl.lit("review"))
              .otherwise(pl.lit("clean")).alias("quality_bucket"),

            # 原因备注
            pl.when(is_non_trade).then(pl.lit("non_trade"))
              .when(has_invalid_price).then(pl.lit("invalid_px"))
              .when(has_invalid_size).then(pl.lit("invalid_sz"))
              .when(dirty_flags).then(pl.lit("bad_ts_recv"))
              .when(has_negative_latency).then(pl.lit("negative_latency"))
              .when(has_unknown_side).then(pl.lit("unknown_side"))
              .otherwise(pl.lit(None)).alias("drop_reason"),

            pl.when(dirty_flags).then(pl.lit(True)).otherwise(pl.lit(False)).alias("has_flag_warning")
        ])
    )


    clean_out_dir = get_processed_path(STAGE4_DIR, asset_type, 'trades', trade_date)
    clean_out_path = clean_out_dir / "clean_trades.parquet"

    qc_out_dir = get_processed_path(STAGE4_DIR, asset_type, 'trade_qc', trade_date)
    qc_out_path = qc_out_dir / "trade_qc_detail.parquet"

    qc_detail = cleaned.select([
        pl.lit(trade_date).alias("trade_date"),
        pl.lit(asset_type).alias("asset_type"),
        "contract_key",
        "symbol",
        "underlying_symbol",
        "publisher_id",
        "instrument_id",
        "record_id",
        "ts_recv_ns",
        "ts_event_ns",
        "sequence",
        pl.col("action").alias("raw_action"),
        pl.col("side").alias("raw_side"),
        pl.col("price").alias("raw_price"),
        pl.col("size").alias("raw_size"),
        pl.col("flags").alias("raw_flags"),
        "is_non_trade",
        "has_invalid_price",
        "has_invalid_size",
        "has_unknown_side",
        "has_negative_latency",
        "has_bad_ts_recv",
        "is_valid_trade",
        pl.col("quality_bucket").alias("qc_bucket"),
        pl.col("drop_reason").alias("qc_reason_code"),
        "stage_version",
    ])

    cleaned.sink_parquet(clean_out_path, compression="zstd")
    qc_detail.sink_parquet(qc_out_path, compression="zstd")
    logger.info(
        "Finished Cleaning Trades -> %s | QC Detail -> %s",
        clean_out_path.name,
        qc_out_path.name,
    )

def run_stage4_clean(trade_dates: list[str]):
    for d in trade_dates:
        for asset in ['und', 'opt']:
            clean_trades(d, asset)

if __name__ == "__main__":
    import sys
    dates = sys.argv[1:] if len(sys.argv) > 1 else ["2026-05-12"]
    run_stage4_clean(dates)
