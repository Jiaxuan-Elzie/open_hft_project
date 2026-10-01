import logging
import time
from pathlib import Path

import polars as pl

from config import STAGE4_DIR, STAGE5_DIR, STAGE6_DIR, STAGE7_DIR, get_processed_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_qc_report(trade_date: str, asset_type: str) -> None:
    """Stage 7: 汇总当天的对齐率及质量桶等量化指标 (严格遵循 MD 阈值要求)"""
    stage_start_time = time.perf_counter()
    trades_path = get_processed_path(STAGE6_DIR, asset_type, 'aligned', trade_date) / "aligned_trade_book.parquet"
    book_path = get_processed_path(STAGE5_DIR, asset_type, 'mbp-10', trade_date) / "clean_mbp10.parquet"

    if not trades_path.exists():
        logger.warning(f"Skipping QC for {trade_date} - {asset_type}: No aligned trades found at {trades_path}")
        return

    if not book_path.exists():
        logger.warning(f"Skipping QC for {trade_date} - {asset_type}: No clean book found at {book_path}")
        return

    logger.info(
        "Generating QC Reports for %s - %s | aligned=%s (%.2f MB) | book=%s (%.2f MB)",
        trade_date,
        asset_type,
        trades_path,
        trades_path.stat().st_size / (1024 * 1024),
        book_path,
        book_path.stat().st_size / (1024 * 1024),
    )

    read_start_time = time.perf_counter()
    logger.info("Reading Stage 7 inputs for %s - %s", trade_date, asset_type)

    align_lf = pl.scan_parquet(trades_path)
    mbp10_lf = pl.scan_parquet(book_path)
    logger.info(
        "Finished Stage 7 input scan setup for %s - %s in %.2f seconds",
        trade_date,
        asset_type,
        time.perf_counter() - read_start_time,
    )

    # Trades 维度质量统计 (包含 SOV - Trade & Book 对齐后的统计)
    plan_start_time = time.perf_counter()
    logger.info("Building Stage 7 QC plan for %s - %s", trade_date, asset_type)
    trades_stats = (
        align_lf.group_by(["contract_key", "symbol", "underlying_symbol"])
        .agg([
            pl.len().alias("total_trades"),
            # side == 'N' 占比 (RTH) < 10%
            pl.col("trade_side").filter(pl.col("is_rth")).eq("N").sum().alias("rth_side_n_count"),
            pl.col("is_rth").sum().alias("rth_trades_count"),

            # F_BAD_TS_RECV 占比 < 0.1%
            pl.col("has_bad_ts_recv").sum().alias("bad_ts_recv_count"),

            # trade @ or within BBO 占比 > 95%
            ((pl.col("price") >= pl.col("best_bid_px")) & (pl.col("price") <= pl.col("best_ask_px"))).sum().alias("within_bbo_count"),

            # stale BBO trade (BBO age > 100ms) 占比 < 5%
            # book_age_ns > 100,000,000 ns (100ms)
            (pl.col("book_age_ns") > 100_000_000).sum().alias("stale_bbo_count"),

            # SOV: quote rule 与 db_sign 一致率 > 90%
            # 只有当 db_sign 不为 0 且 qr_sign 不为 0 时才计算一致性
            (
                (pl.col("db_sign") != 0) &
                (pl.col("qr_sign") != 0) &
                (pl.col("db_sign") == pl.col("qr_sign"))
            ).sum().alias("sov_consistent_count"),

            (
                (pl.col("db_sign") != 0) &
                (pl.col("qr_sign") != 0)
            ).sum().alias("sov_total_count"),

            # 保留基础 publishable 阈值 (clean trades > 95%)
            (pl.col("quality_bucket") == "clean").sum().alias("clean_trades_count")
        ])
    )

    # MBP-10 维度质量统计
    mbp10_stats = (
        mbp10_lf.group_by(["contract_key"])
        .agg([
            pl.len().alias("total_mbp10"),

            # 单 venue crossed 占比 < 0.01%
            pl.col("is_crossed_book").sum().alias("crossed_mbp10_count"),

            # levels monotonicity 违反 < 0.1%
            pl.col("is_non_monotonic").sum().alias("non_mono_mbp10_count"),

            # ts_in_delta p99 < 1ms (1,000,000 ns)
            pl.col("recv_event_latency_ns").cast(pl.Int64).quantile(0.99).alias("ts_in_delta_p99_ns")
        ])
    )

    # 关联两边统计结果并计算百分比
    qc_summary = (
        trades_stats.join(mbp10_stats, on="contract_key", how="left")
        .with_columns([
            pl.lit(trade_date).alias("trade_date"),
            pl.lit(asset_type).alias("asset_type"),

            # 比例计算
            (pl.col("rth_side_n_count") / pl.col("rth_trades_count")).fill_nan(0.0).alias("trades_side_n_pct"),
            (pl.col("bad_ts_recv_count") / pl.col("total_trades")).fill_nan(0.0).alias("trades_bad_ts_pct"),
            (pl.col("within_bbo_count") / pl.col("total_trades")).fill_nan(0.0).alias("trades_within_bbo_pct"),
            (pl.col("stale_bbo_count") / pl.col("total_trades")).fill_nan(0.0).alias("stale_bbo_trade_pct"),

            (pl.col("sov_consistent_count") / pl.col("sov_total_count")).fill_nan(0.0).alias("sov_consistent_pct"),

            (pl.col("crossed_mbp10_count") / pl.col("total_mbp10")).fill_nan(0.0).alias("mbp10_crossed_pct"),
            (pl.col("non_mono_mbp10_count") / pl.col("total_mbp10")).fill_nan(0.0).alias("mbp10_non_mono_pct")
        ])
        .with_columns([
            # 自动化检查告警标记 (Boolean)
            (pl.col("trades_side_n_pct") < 0.10).alias("chk_pass_side_n"),
            (pl.col("trades_bad_ts_pct") < 0.001).alias("chk_pass_bad_ts"),
            (pl.col("trades_within_bbo_pct") > 0.95).alias("chk_pass_within_bbo"),
            (pl.col("mbp10_crossed_pct") < 0.0001).alias("chk_pass_mbp10_crossed"),
            (pl.col("mbp10_non_mono_pct") < 0.001).alias("chk_pass_mbp10_mono"),
            (pl.col("ts_in_delta_p99_ns") < 1_000_000).alias("chk_pass_latency"),
            (pl.col("stale_bbo_trade_pct") < 0.05).alias("chk_pass_stale_bbo"),
            (pl.col("sov_consistent_pct") > 0.90).alias("chk_pass_sov"),

            (pl.col("clean_trades_count") / pl.col("total_trades") > 0.95).alias("publishable"),
            pl.lit("v1.1").alias("stage_version")
        ])
    )
    logger.info(
        "Stage 7 QC plan ready for %s - %s in %.2f seconds",
        trade_date,
        asset_type,
        time.perf_counter() - plan_start_time,
    )

    out_dir = get_processed_path(STAGE7_DIR, asset_type, 'qc', trade_date)
    out_path = out_dir / "qc_summary.csv"

    write_start_time = time.perf_counter()
    logger.info("Starting Stage 7 collect/write for %s - %s -> %s", trade_date, asset_type, out_path)

    # 汇总表直接输出 csv 供人类审阅
    qc_summary.collect().write_csv(out_path)
    logger.info(
        "Finished generating QC for %s - %s -> %s | write_time=%.2f seconds | total_time=%.2f seconds | output_size=%.2f KB",
        trade_date,
        asset_type,
        out_path.name,
        time.perf_counter() - write_start_time,
        time.perf_counter() - stage_start_time,
        out_path.stat().st_size / 1024,
    )

def run_stage7_qc(trade_dates: list[str]):
    for d in trade_dates:
        for asset in ['und', 'opt']:
            generate_qc_report(d, asset)

if __name__ == "__main__":
    import sys
    dates = sys.argv[1:] if len(sys.argv) > 1 else ["2026-05-12"]
    run_stage7_qc(dates)
