import logging
import os
import time
from datetime import date

import duckdb
import polars as pl

from config import STAGE1_DIR, STAGE2_DIR, STAGE3_DIR, TRADING_CALENDAR_DIR, get_processed_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CALENDAR_INTERVALS_PATH = TRADING_CALENDAR_DIR / "session_calendar_by_utc_day.parquet"
DUCKDB_MEMORY_LIMIT = os.getenv("STAGE3_DUCKDB_MEMORY_LIMIT", "16GB")


def _duckdb_literal(value: str) -> str:
    return value.replace("'", "''")


def load_calendar_day(utc_date: str) -> pl.DataFrame:
    if not CALENDAR_INTERVALS_PATH.exists():
        raise FileNotFoundError(
            f"Trading calendar file not found at {CALENDAR_INTERVALS_PATH}. "
            "Run build_trading_calendar.py first."
        )

    return (
        pl.read_parquet(CALENDAR_INTERVALS_PATH)
        .filter(pl.col("utc_date") == pl.lit(date.fromisoformat(utc_date)))
        .sort(["product_root", "interval_start_utc", "segment_index"])
        .select([
            "product_root",
            "calendar_trade_date",
            pl.col("interval_start_utc").dt.epoch("ns").alias("interval_start_ns"),
            pl.col("interval_end_utc").dt.epoch("ns").alias("interval_end_ns"),
            "calendar_name",
        ])
    )


def _build_sessionize_select_sql(stage1_glob: str) -> str:
    stage1_glob_literal = _duckdb_literal(stage1_glob)
    return f"""
WITH base AS (
    SELECT
        stage1.* EXCLUDE (ts_recv_ns, ts_event_ns),
        epoch_ns(stage1.ts_recv_ns) AS ts_recv_ns,
        epoch_ns(stage1.ts_event_ns) AS ts_event_ns,
        COALESCE(sym.contract_key, stage1.symbol) AS contract_key,
        sym.underlying_symbol,
        NULLIF(
            regexp_extract(
                split_part(COALESCE(sym.underlying_symbol, sym.contract_key, stage1.symbol), ' ', 1),
                '^([A-Z]+)[FGHJKMNQUVXZ][0-9]',
                1
            ),
            ''
        ) AS product_root
    FROM read_parquet('{stage1_glob_literal}') AS stage1
    LEFT JOIN symbology AS sym
      ON stage1.instrument_id = sym.instrument_id
     AND stage1.publisher_id = sym.publisher_id
),
annotated AS (
    SELECT
        base.*,
        CASE WHEN cal.product_root IS NOT NULL THEN TRUE ELSE FALSE END AS is_rth,
        CASE WHEN cal.product_root IS NOT NULL THEN 'RTH' ELSE 'CLOSED' END AS session_name,
        cal.calendar_trade_date,
        cal.interval_start_ns AS session_open_utc,
        cal.interval_end_ns AS session_close_utc,
        cal.calendar_name
    FROM base
    LEFT JOIN calendar_day AS cal
      ON base.product_root = cal.product_root
      AND base.ts_recv_ns >= cal.interval_start_ns
      AND base.ts_recv_ns < cal.interval_end_ns
)
SELECT
    annotated.*,
    annotated.ts_recv_ns - annotated.ts_event_ns AS recv_event_latency_ns,
    concat(
        CAST(annotated.contract_key AS VARCHAR),
        '_',
        CAST(annotated.ts_recv_ns AS VARCHAR),
        '_',
        CAST(annotated.sequence AS VARCHAR)
    ) AS record_id
FROM annotated
ORDER BY contract_key, ts_recv_ns, sequence
"""


def build_sessionized(trade_date: str, schema: str, asset_type: str) -> None:
    """Stage 3: 连表、打标、截取、生成 record_id"""
    job_start_time = time.perf_counter()

    # 1. 寻找 stage 1 源数据 (Lazy Scan)
    in_dir = get_processed_path(STAGE1_DIR, asset_type, schema, trade_date)
    if not in_dir.exists():
        logger.warning(f"No Stage1 input found at {in_dir}")
        return

    input_files = sorted(in_dir.glob("*.parquet"))
    logger.info(
        "Stage 3 input ready for %s %s %s: dir=%s file_count=%s",
        trade_date,
        asset_type,
        schema,
        in_dir,
        len(input_files),
    )

    sym_dir = STAGE2_DIR / trade_date
    if not sym_dir.exists():
        logger.warning(f"No Symbology found at {sym_dir}")
        return

    sym_path = sym_dir / "symbology.parquet"
    sym_df = pl.read_parquet(sym_path).select([
        "instrument_id",
        "publisher_id",
        "contract_key",
        "underlying_symbol",
    ])
    calendar_day = load_calendar_day(trade_date)
    calendar_roots = calendar_day.get_column("product_root").drop_nulls().unique(maintain_order=True).to_list()

    logger.info(
        "Sessionizing %s %s for %s using persisted trading calendar at %s",
        schema,
        asset_type,
        trade_date,
        CALENDAR_INTERVALS_PATH,
    )
    logger.info(
        "Loaded trading calendar intervals for %s: product_roots=%s",
        trade_date,
        calendar_roots,
    )
    logger.info(
        "Loaded symbology/calendar for %s %s %s: symbology_rows=%s calendar_rows=%s",
        trade_date,
        asset_type,
        schema,
        sym_df.height,
        calendar_day.height,
    )

    logger.info("Building Stage 3 join plan for %s %s %s", trade_date, asset_type, schema)
    calendar_attach_start = time.perf_counter()
    logger.info(
        "Attaching calendar columns for %s %s %s across %s configured product roots",
        trade_date,
        asset_type,
        schema,
        len(calendar_roots),
    )
    logger.info(
        "Calendar column plan ready for %s %s %s in %.2f seconds",
        trade_date,
        asset_type,
        schema,
        time.perf_counter() - calendar_attach_start,
    )

    logger.info(
        "Preparing global sort for %s %s %s by [contract_key, ts_recv_ns, sequence]",
        trade_date,
        asset_type,
        schema,
    )

    out_dir = get_processed_path(STAGE3_DIR, asset_type, schema, trade_date)
    out_path = out_dir / "sessionized.parquet"
    temp_out_path = out_dir / "sessionized.duckdb_tmp.parquet"
    temp_dir = TRADING_CALENDAR_DIR.parent / "tmp" / "duckdb_stage3"
    temp_dir.mkdir(parents=True, exist_ok=True)
    stage1_glob = str(in_dir / "*.parquet")
    select_sql = _build_sessionize_select_sql(stage1_glob)
    copy_sql = (
        "COPY ("
        f"{select_sql}"
        ") TO '"
        f"{_duckdb_literal(str(temp_out_path))}"
        "' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )

    sink_start = time.perf_counter()
    logger.info("Starting sink_parquet for %s %s %s -> %s", trade_date, asset_type, schema, out_path)
    if out_path.exists():
        out_path.unlink()
    if temp_out_path.exists():
        temp_out_path.unlink()

    connection = duckdb.connect()
    try:
        connection.execute(f"SET temp_directory = '{_duckdb_literal(str(temp_dir))}'")
        connection.execute(f"SET memory_limit = '{_duckdb_literal(DUCKDB_MEMORY_LIMIT)}'")
        connection.register("symbology", sym_df.to_arrow())
        connection.register("calendar_day", calendar_day.to_arrow())
        connection.execute(copy_sql)
    finally:
        connection.close()

    logger.info("Rewriting DuckDB output with Polars to restore datetime[ns, UTC] columns for %s %s %s", trade_date, asset_type, schema)
    (
        pl.scan_parquet(temp_out_path)
        .with_columns([
            pl.from_epoch("ts_recv_ns", time_unit="ns").dt.replace_time_zone("UTC").alias("ts_recv_ns"),
            pl.from_epoch("ts_event_ns", time_unit="ns").dt.replace_time_zone("UTC").alias("ts_event_ns"),
            pl.from_epoch("session_open_utc", time_unit="ns").dt.replace_time_zone("UTC").alias("session_open_utc"),
            pl.from_epoch("session_close_utc", time_unit="ns").dt.replace_time_zone("UTC").alias("session_close_utc"),
        ])
        .sink_parquet(out_path, compression="zstd")
    )
    temp_out_path.unlink(missing_ok=True)

    logger.info(
        "Finished Sessionizing for %s %s %s -> %s | sink_time=%.2f seconds | total_time=%.2f seconds",
        trade_date,
        asset_type,
        schema,
        out_path.name,
        time.perf_counter() - sink_start,
        time.perf_counter() - job_start_time,
    )

def run_stage3_sessionize(trade_dates: list[str]):
    logger.info(f"--- Starting Stage 3 Sessionize for {trade_dates} ---")
    for d in trade_dates:
        for asset in ['und', 'opt']:
            for schema in ['trades', 'mbp-10']:
                build_sessionized(d, schema, asset)

if __name__ == "__main__":
    import sys
    dates = sys.argv[1:] if len(sys.argv) > 1 else ["2026-05-12"]
    run_stage3_sessionize(dates)
