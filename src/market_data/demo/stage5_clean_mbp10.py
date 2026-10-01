import logging
import os
import time

import polars as pl

from config import STAGE3_DIR, STAGE5_DIR, get_processed_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

F_BAD_TS_RECV = 1 << 4
F_SNAPSHOT = 1 << 5  # Databento snapshot flag
WIDE_SPREAD_ABS_THRESHOLD = 20.0
WIDE_SPREAD_BPS_THRESHOLD = 50.0

def clean_mbp10(trade_date: str, asset_type: str) -> None:
    """Stage 5: 对盘口做基本检验，如 crossed 检查、单调性检查"""
    stage_start_time = time.perf_counter()
    in_dir = get_processed_path(STAGE3_DIR, asset_type, 'mbp-10', trade_date)
    in_path = in_dir / "sessionized.parquet"

    # 检查是否存在上游数据
    if not in_path.exists():
        logger.warning(f"Skipping MBP10 cleaning for {trade_date} - {asset_type}: No input file at {in_path}")
        return

    logger.info(
        "Cleaning MBP10 for %s - %s | input=%s | input_size=%.2f MB",
        trade_date,
        asset_type,
        in_path,
        in_path.stat().st_size / (1024 * 1024),
    )

    # 这里为了解决之前 sessionized.parquet 读取报错的问题，我们使用 read_parquet
    read_start_time = time.perf_counter()
    logger.info("Reading Stage 5 input for %s - %s", trade_date, asset_type)
    try:
        lf = pl.read_parquet(in_path).lazy()
    except Exception as e:
        logger.error(f"Error reading {in_path}: {e}")
        return
    logger.info(
        "Finished reading Stage 5 input for %s - %s in %.2f seconds",
        trade_date,
        asset_type,
        time.perf_counter() - read_start_time,
    )

    # 1) 剔 ts_recv 不可信
    lf = lf.filter((pl.col('flags') & F_BAD_TS_RECV) == 0)

    # 提取 Top of book
    best_bid_px = pl.col("bid_px_00")
    best_ask_px = pl.col("ask_px_00")
    best_bid_sz = pl.col("bid_sz_00")
    best_ask_sz = pl.col("ask_sz_00")

    # 2) 单 venue 内 crossed 检测
    is_crossed = (best_bid_px > 0) & (best_ask_px > 0) & (best_bid_px >= best_ask_px)

    # 3) Levels monotonicity: bid 递减、ask 递增。
    # Databento 在空档位上会给出 null price + 0 size/count。
    # 如果不显式处理这些 null，Polars 的布尔聚合会把结果抬成 null，
    # 最终使大量稀疏盘口错误地落入 clean。
    mono_conditions = []
    bid_gap_conditions = []
    ask_gap_conditions = []
    bid_present_levels = []
    ask_present_levels = []
    seen_missing_bid = pl.lit(False)
    seen_missing_ask = pl.lit(False)

    for i in range(9):
        curr_bid = pl.col(f"bid_px_0{i}")
        next_bid = pl.col(f"bid_px_0{i+1}")
        curr_ask = pl.col(f"ask_px_0{i}")
        next_ask = pl.col(f"ask_px_0{i+1}")

        curr_bid_present = curr_bid.is_not_null()
        next_bid_present = next_bid.is_not_null()
        curr_ask_present = curr_ask.is_not_null()
        next_ask_present = next_ask.is_not_null()

        bid_present_levels.append(curr_bid_present.cast(pl.UInt8))
        ask_present_levels.append(curr_ask_present.cast(pl.UInt8))

        mono_conditions.append(
            (curr_bid_present & next_bid_present & (next_bid >= curr_bid)).fill_null(False)
        )
        mono_conditions.append(
            (curr_ask_present & next_ask_present & (next_ask <= curr_ask)).fill_null(False)
        )

        seen_missing_bid_next = seen_missing_bid | curr_bid.is_null()
        seen_missing_ask_next = seen_missing_ask | curr_ask.is_null()

        bid_gap_conditions.append((seen_missing_bid_next & next_bid_present).fill_null(False))
        ask_gap_conditions.append((seen_missing_ask_next & next_ask_present).fill_null(False))

        seen_missing_bid = seen_missing_bid_next
        seen_missing_ask = seen_missing_ask_next

    bid_present_levels.append(pl.col("bid_px_09").is_not_null().cast(pl.UInt8))
    ask_present_levels.append(pl.col("ask_px_09").is_not_null().cast(pl.UInt8))

    is_non_monotonic = pl.any_horizontal(*mono_conditions).fill_null(False)
    bid_level_count = sum(bid_present_levels)
    ask_level_count = sum(ask_present_levels)
    has_bid_gap = pl.any_horizontal(*bid_gap_conditions).fill_null(False)
    has_ask_gap = pl.any_horizontal(*ask_gap_conditions).fill_null(False)
    is_sparse = (bid_level_count <= 1) | (ask_level_count <= 1) | has_bid_gap | has_ask_gap

    # 4) Snapshot / clear 保留 (但不计入正常状态统计)
    is_snapshot = (pl.col("action") == "R") | ((pl.col("flags") & F_SNAPSHOT) != 0)

    # 空盘检测
    is_empty = (best_bid_px == 0) | (best_ask_px == 0)

    spread_expr = pl.when(~is_empty).then(best_ask_px - best_bid_px).otherwise(pl.lit(None))
    mid_expr = pl.when(~is_empty).then((best_bid_px + best_ask_px) / 2.0).otherwise(pl.lit(None))
    spread_bps_expr = (
        pl.when(mid_expr.is_not_null() & (mid_expr > 0))
        .then(spread_expr / mid_expr * 10_000)
        .otherwise(pl.lit(None))
    )
    is_wide = (
        spread_expr.is_not_null() &
        (
            (spread_expr > WIDE_SPREAD_ABS_THRESHOLD) |
            (spread_bps_expr > WIDE_SPREAD_BPS_THRESHOLD)
        )
    ).fill_null(False)

    plan_start_time = time.perf_counter()
    logger.info("Building Stage 5 clean plan for %s - %s", trade_date, asset_type)

    # 构造清洗后的 LazyFrame
    cleaned = (
        lf
        .with_columns([
            best_bid_px.alias("best_bid_px"),
            best_ask_px.alias("best_ask_px"),
            best_bid_sz.alias("best_bid_sz"),
            best_ask_sz.alias("best_ask_sz"),

            spread_expr.alias("spread"),
            mid_expr.alias("mid_px"),

            is_crossed.alias("is_crossed_book"),
            is_empty.alias("is_empty_book"),
            is_non_monotonic.alias("is_non_monotonic"),
            is_sparse.alias("is_sparse_book"),
            is_wide.alias("is_wide_book"),
            is_snapshot.alias("is_snapshot"),

            # 分桶
            pl.when(is_empty).then(pl.lit("drop"))
              .when(is_crossed | is_non_monotonic | is_sparse | is_wide).then(pl.lit("review"))
              .otherwise(pl.lit("clean")).alias("book_quality_bucket"),

            pl.lit("v1.0").alias("stage_version")
        ])
        # 去掉 .sort()，因为前置 Stage 3 已经严格排序过了。
        # 且 filter 和 with_columns 操作本身是保序的。
        # 去掉 sort 可以使下游完全支持纯流式计算，防止爆内存！
    )
    logger.info(
        "Stage 5 clean plan ready for %s - %s in %.2f seconds",
        trade_date,
        asset_type,
        time.perf_counter() - plan_start_time,
    )

    #region debug-point stage5-book-summary
    if os.getenv("STAGE5_DEBUG_SUMMARY") == "1":
        debug_summary = cleaned.select([
            pl.len().alias("total_rows"),
            (pl.col("spread") < 0).sum().alias("neg_spread_rows"),
            (pl.col("spread") > 20).sum().alias("wide_spread_rows"),
            pl.col("is_crossed_book").sum().alias("crossed_rows"),
            pl.col("is_sparse_book").sum().alias("sparse_rows"),
            pl.col("is_wide_book").sum().alias("wide_flag_rows"),
            pl.col("is_non_monotonic").is_null().sum().alias("null_mono_rows"),
        ]).collect().row(0)
        logger.info(
            "Stage 5 debug summary for %s - %s | total=%s neg=%s wide=%s crossed=%s sparse=%s wide_flag=%s null_mono=%s",
            trade_date,
            asset_type,
            *debug_summary,
        )
    #endregion

    out_dir = get_processed_path(STAGE5_DIR, asset_type, 'mbp-10', trade_date)
    out_path = out_dir / "clean_mbp10.parquet"
    write_start_time = time.perf_counter()
    logger.info("Starting Stage 5 sink_parquet for %s - %s -> %s", trade_date, asset_type, out_path)

    # 真正激活流式写入 (Streaming):
    # collect() 会把整个数据集强行拉进内存，然后执行 write_parquet，这在数据量大时必爆内存。
    # sink_parquet() 则是流式的，处理一批写入一批，内存开销极小。
    cleaned.sink_parquet(out_path, compression="zstd")

    logger.info(
        "Finished Cleaning MBP10 for %s - %s -> %s | write_time=%.2f seconds | total_time=%.2f seconds | output_size=%.2f MB",
        trade_date,
        asset_type,
        out_path.name,
        time.perf_counter() - write_start_time,
        time.perf_counter() - stage_start_time,
        out_path.stat().st_size / (1024 * 1024),
    )

def run_stage5_clean(trade_dates: list[str]):
    for d in trade_dates:
        for asset in ['und', 'opt']:
            clean_mbp10(d, asset)

if __name__ == "__main__":
    import sys
    dates = sys.argv[1:] if len(sys.argv) > 1 else ["2026-05-12"]
    run_stage5_clean(dates)
