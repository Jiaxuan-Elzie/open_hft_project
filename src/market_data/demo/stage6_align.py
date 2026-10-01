import logging
import re
import time
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from config import STAGE2_DIR, STAGE4_DIR, STAGE5_DIR, STAGE6_DIR, get_processed_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

LEGACY_ALIGNMENT_TOLERANCE_NS = 1_000_000_000
DEFAULT_MIN_TRADE_VOLUME = 0
SCHEMA_DIR = Path(__file__).with_name("schema")
COLUMNS_SCHEMA_PATH = SCHEMA_DIR / "columns.csv"
STAGE2_REVISED_DAILY_DIR = STAGE2_DIR.parent / "stage2_symbology_revised" / "daily"


@dataclass(slots=True)
class UniverseFilters:
    min_trade_volume: int = DEFAULT_MIN_TRADE_VOLUME
    min_trade_notional: float = 0.0
    call_put: str | None = None
    expiry_start: str | None = None
    expiry_end: str | None = None
    atm_top_n: int | None = None
    max_contracts: int | None = None
    contract_list: list[str] | None = None


@dataclass(slots=True)
class Stage6ColumnOverrides:
    fut_mbp_extra_cols: list[str] | None = None
    fut_mbp_drop_cols: list[str] | None = None
    opt_mbp_extra_cols: list[str] | None = None
    opt_mbp_drop_cols: list[str] | None = None
    fut_trade_extra_cols: list[str] | None = None
    fut_trade_drop_cols: list[str] | None = None
    opt_trade_extra_cols: list[str] | None = None
    opt_trade_drop_cols: list[str] | None = None


@lru_cache(maxsize=1)
def _load_columns_schema() -> pl.DataFrame:
    if not COLUMNS_SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Missing columns schema file: {COLUMNS_SCHEMA_PATH}")
    return pl.read_csv(
        COLUMNS_SCHEMA_PATH,
        schema_overrides={"load_by_default": pl.Boolean},
    )


def _resolve_schema_columns(
    source: str,
    available_cols: list[str],
    extra_cols: list[str] | None = None,
    drop_cols: list[str] | None = None,
) -> list[str]:
    schema_df = _load_columns_schema()
    selected = (
        schema_df
        .filter((pl.col("source") == source) & pl.col("load_by_default"))
        .get_column("column_name")
        .to_list()
    )
    if not selected:
        raise ValueError(f"No default columns configured for source={source}")

    ordered: list[str] = [col for col in selected if col in available_cols]
    if extra_cols:
        for col in extra_cols:
            if col in available_cols and col not in ordered:
                ordered.append(col)
    if drop_cols:
        dropped = set(drop_cols)
        ordered = [col for col in ordered if col not in dropped]
    return ordered


def read_parquet_with_schema(
    path: Path,
    source: str,
    extra_cols: list[str] | None = None,
    drop_cols: list[str] | None = None,
    n_rows: int | None = None,
) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    available_cols = pl.read_parquet(path, n_rows=1).columns
    columns = _resolve_schema_columns(source, available_cols, extra_cols=extra_cols, drop_cols=drop_cols)
    return pl.read_parquet(path, columns=columns, n_rows=n_rows)


def _sort_columns(df: pl.DataFrame, include_contract: bool) -> list[str]:
    order: list[str] = []
    if include_contract and "contract_key" in df.columns:
        order.append("contract_key")
    for col in ("ts_recv_ns", "sequence", "record_id"):
        if col in df.columns:
            order.append(col)
    return order


def _datetime_ns_array(series: pl.Series) -> np.ndarray:
    return series.dt.epoch("ns").to_numpy()


def _searchsorted_last(event_ts_ns: np.ndarray, base_ts_ns: np.ndarray) -> np.ndarray:
    if event_ts_ns.size == 0:
        return np.full(base_ts_ns.shape[0], -1, dtype=np.int64)
    return np.searchsorted(event_ts_ns, base_ts_ns, side="right").astype(np.int64) - 1


def _safe_contract_filename(contract_key: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", contract_key).strip("_")


def _stage_path(stage_dir: Path, schema: str, asset_type: str, trade_date: str, file_name: str) -> Path:
    return stage_dir / schema / asset_type / trade_date / file_name


def _load_frame(
    path: Path,
    source: str,
    filter_contracts: list[str] | None = None,
    extra_cols: list[str] | None = None,
    drop_cols: list[str] | None = None,
) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()

    available_cols = pl.read_parquet(path, n_rows=1).columns
    columns = _resolve_schema_columns(source, available_cols, extra_cols=extra_cols, drop_cols=drop_cols)

    if filter_contracts:
        lf = pl.scan_parquet(path)
        lf = lf.select(columns)
        return lf.filter(pl.col("contract_key").is_in(filter_contracts)).collect()

    return pl.read_parquet(path, columns=columns)


def _add_global_row(df: pl.DataFrame, row_col: str) -> pl.DataFrame:
    if df.is_empty():
        return df.with_columns(pl.Series(row_col, [], dtype=pl.Int64))
    return df.with_row_index(name=row_col)


def _add_group_row(df: pl.DataFrame, row_col: str) -> pl.DataFrame:
    if df.is_empty():
        return df.with_columns(pl.Series(row_col, [], dtype=pl.Int64))
    return df.with_columns(
        (pl.col("contract_key").cum_count().over("contract_key") - 1)
        .cast(pl.Int64)
        .alias(row_col)
    )


def _load_symbology(trade_date: str) -> pl.DataFrame:
    revised_path: Path | None = None
    if STAGE2_REVISED_DAILY_DIR.exists():
        candidate_dirs = sorted(
            path for path in STAGE2_REVISED_DAILY_DIR.iterdir()
            if path.is_dir() and path.name <= trade_date
        )
        if candidate_dirs:
            revised_path = candidate_dirs[-1] / "symbology.parquet"

    if revised_path is not None and revised_path.exists():
        logger.info("Loading revised symbology for %s from %s", trade_date, revised_path)
        return pl.read_parquet(revised_path)

    legacy_path = STAGE2_DIR / trade_date / "symbology.parquet"
    if legacy_path.exists():
        logger.warning(
            "Revised symbology not found for %s; falling back to legacy snapshot %s",
            trade_date,
            legacy_path,
        )
        return pl.read_parquet(legacy_path)

    logger.warning("No symbology snapshot found for %s in revised or legacy outputs", trade_date)
    return pl.DataFrame()


def _option_itm_otm_bucket_expr(ref_mid: float) -> pl.Expr:
    ref_mid_lit = pl.lit(ref_mid)
    return (
        pl.when((pl.col("option_type") == "C") & (pl.col("strike") <= ref_mid_lit)).then(pl.lit("itm"))
        .when((pl.col("option_type") == "C") & (pl.col("strike") > ref_mid_lit)).then(pl.lit("otm"))
        .when((pl.col("option_type") == "P") & (pl.col("strike") >= ref_mid_lit)).then(pl.lit("itm"))
        .when((pl.col("option_type") == "P") & (pl.col("strike") < ref_mid_lit)).then(pl.lit("otm"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )


def build_contract_universe(
    trade_date: str,
    fut_mbp_df: pl.DataFrame,
    opt_trade_df: pl.DataFrame,
    filters: UniverseFilters,
) -> pl.DataFrame:
    trade_stats = (
        opt_trade_df.group_by(["contract_key"])
        .agg([
            pl.col("symbol").first().alias("symbol"),
            pl.len().alias("trade_count"),
            pl.col("size").sum().alias("trade_volume"),
            pl.col("notional").sum().alias("trade_notional"),
        ])
        .sort(["trade_volume", "trade_count", "contract_key"], descending=[True, True, False])
    )

    symbology = _load_symbology(trade_date)
    if not symbology.is_empty():
        fut_underlyings = (
            fut_mbp_df.select(pl.col("symbol").alias("underlying_symbol"))
            .drop_nulls()
            .unique()
        )

        symbology = (
            symbology.filter(pl.col("asset_type") == "opt")
            .select([
                "contract_key",
                "symbol",
                "underlying_symbol",
                "expiry_date",
                "option_type",
                "strike",
            ])
            .unique(subset=["contract_key"], keep="first")
        )

        if not fut_underlyings.is_empty():
            symbology = symbology.join(fut_underlyings, on="underlying_symbol", how="inner")

        universe = (
            symbology.join(
                trade_stats.select(["contract_key", "trade_count", "trade_volume", "trade_notional"]),
                on="contract_key",
                how="left",
            )
            .with_columns([
                pl.col("trade_count").fill_null(0),
                pl.col("trade_volume").fill_null(0),
                pl.col("trade_notional").fill_null(0.0),
            ])
        )
    else:
        universe = trade_stats

    if filters.contract_list:
        universe = universe.filter(pl.col("contract_key").is_in(filters.contract_list))
    if filters.min_trade_volume > 0:
        universe = universe.filter(pl.col("trade_volume") >= filters.min_trade_volume)
    if filters.min_trade_notional > 0:
        universe = universe.filter(pl.col("trade_notional") >= filters.min_trade_notional)
    if filters.call_put:
        universe = universe.filter(pl.col("option_type") == filters.call_put)
    if filters.expiry_start:
        universe = universe.filter(pl.col("expiry_date") >= pl.lit(date.fromisoformat(filters.expiry_start)))
    if filters.expiry_end:
        universe = universe.filter(pl.col("expiry_date") <= pl.lit(date.fromisoformat(filters.expiry_end)))

    if filters.atm_top_n is not None and "strike" in universe.columns and "mid_px" in fut_mbp_df.columns:
        fut_ref_mid = (
            fut_mbp_df.filter(pl.col("symbol").is_not_null() & pl.col("mid_px").is_not_null())
            .group_by("symbol")
            .agg(pl.col("mid_px").last().alias("ref_mid"))
            .rename({"symbol": "underlying_symbol"})
        )
        if not fut_ref_mid.is_empty():
            universe = universe.join(fut_ref_mid, on="underlying_symbol", how="left")
            universe = (
                universe.filter(pl.col("ref_mid").is_not_null())
                .with_columns([
                    (pl.col("strike") - pl.col("ref_mid")).abs().alias("strike_distance"),
                    pl.when((pl.col("option_type") == "C") & (pl.col("strike") <= pl.col("ref_mid"))).then(pl.lit("itm"))
                    .when((pl.col("option_type") == "C") & (pl.col("strike") > pl.col("ref_mid"))).then(pl.lit("otm"))
                    .when((pl.col("option_type") == "P") & (pl.col("strike") >= pl.col("ref_mid"))).then(pl.lit("itm"))
                    .when((pl.col("option_type") == "P") & (pl.col("strike") < pl.col("ref_mid"))).then(pl.lit("otm"))
                    .otherwise(pl.lit(None, dtype=pl.Utf8))
                    .alias("moneyness_bucket"),
                ])
                .filter(pl.col("moneyness_bucket").is_not_null())
                .sort(
                    [
                        "underlying_symbol",
                        "expiry_date",
                        "option_type",
                        "moneyness_bucket",
                        "strike_distance",
                        "trade_volume",
                        "contract_key",
                    ],
                    descending=[False, False, False, False, False, True, False],
                )
                .with_columns(
                    (
                        pl.col("contract_key")
                        .cum_count()
                        .over(["underlying_symbol", "expiry_date", "option_type", "moneyness_bucket"]) - 1
                    )
                    .cast(pl.Int64)
                    .alias("bucket_rank")
                )
                .filter(pl.col("bucket_rank") < filters.atm_top_n)
                .drop(["bucket_rank", "ref_mid"])
            )

    if filters.max_contracts is not None:
        universe = universe.head(filters.max_contracts)

    if universe.is_empty():
        return universe

    return universe.with_columns(
        pl.col("contract_key").map_elements(_safe_contract_filename, return_dtype=pl.Utf8).alias("file_key")
    )


def _write_option_contract_files(
    frame: pl.DataFrame,
    out_dir: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for contract_key in frame["contract_key"].unique().to_list():
        file_name = f"{_safe_contract_filename(contract_key)}.parquet"
        (
            frame.filter(pl.col("contract_key") == contract_key)
            .write_parquet(out_dir / file_name, compression="zstd")
        )


def _clear_parquet_files(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for path in out_dir.glob("*.parquet"):
        path.unlink()


def _build_chain_table(
    frame: pl.DataFrame,
    row_col: str,
    chain_row_col: str,
    select_cols: list[str],
) -> pl.DataFrame:
    if frame.is_empty():
        return pl.DataFrame(
            schema={
                "ts_ns": pl.Datetime("ns", "UTC"),
                "contract_id": pl.Utf8,
                row_col: pl.Int64,
                chain_row_col: pl.Int64,
            }
        )
    chain = (
        frame.select(select_cols)
        .sort([col for col in ("ts_recv_ns", "contract_key", row_col) if col in frame.columns])
        .with_row_index(chain_row_col)
        .rename({"ts_recv_ns": "ts_ns", "contract_key": "contract_id"})
    )
    return chain


def _build_per_contract_idx(
    fut_mbp_df: pl.DataFrame,
    option_df: pl.DataFrame,
    universe: pl.DataFrame,
    prefix: str,
) -> pl.DataFrame:
    base_ts = _datetime_ns_array(fut_mbp_df["ts_recv_ns"])
    out: dict[str, Any] = {"mbp_row": fut_mbp_df["mbp_row"].clone()}
    if option_df.is_empty() or universe.is_empty():
        return pl.DataFrame(out)

    for contract_key in universe["contract_key"].to_list():
        sub = option_df.filter(pl.col("contract_key") == contract_key)
        if sub.is_empty():
            out[f"{prefix}{contract_key}"] = pl.Series(f"{prefix}{contract_key}", np.full(len(base_ts), -1, dtype=np.int64))
            continue
        event_ts = _datetime_ns_array(sub["ts_recv_ns"])
        out[f"{prefix}{contract_key}"] = pl.Series(
            f"{prefix}{contract_key}",
            _searchsorted_last(event_ts, base_ts),
        )
    return pl.DataFrame(out)


def _validate_monotonic(series: pl.Series) -> bool:
    arr = series.to_numpy()
    if arr.size <= 1:
        return True
    return bool(np.all(np.diff(arr) >= 0))


def get_last_chain_mbp_event(trade_date: str, mbp_row: int) -> dict[str, Any] | None:
    fut_path = STAGE6_DIR / "futures" / trade_date / "mbp.parquet"
    chain_path = STAGE6_DIR / "options" / trade_date / "option_chain_mbp.parquet"
    if not fut_path.exists() or not chain_path.exists():
        return None
    fut = pl.read_parquet(fut_path, columns=["mbp_row", "last_chain_mbp_row"])
    row = fut.filter(pl.col("mbp_row") == mbp_row)
    if row.is_empty() or int(row["last_chain_mbp_row"][0]) < 0:
        return None
    chain = pl.read_parquet(chain_path)
    event = chain.filter(pl.col("chain_mbp_row") == int(row["last_chain_mbp_row"][0]))
    return None if event.is_empty() else event.row(0, named=True)


def get_last_chain_trade_event(trade_date: str, mbp_row: int) -> dict[str, Any] | None:
    fut_path = STAGE6_DIR / "futures" / trade_date / "mbp.parquet"
    chain_path = STAGE6_DIR / "options" / trade_date / "option_chain_trade.parquet"
    if not fut_path.exists() or not chain_path.exists():
        return None
    fut = pl.read_parquet(fut_path, columns=["mbp_row", "last_chain_trade_row"])
    row = fut.filter(pl.col("mbp_row") == mbp_row)
    if row.is_empty() or int(row["last_chain_trade_row"][0]) < 0:
        return None
    chain = pl.read_parquet(chain_path)
    event = chain.filter(pl.col("chain_trade_row") == int(row["last_chain_trade_row"][0]))
    return None if event.is_empty() else event.row(0, named=True)


def get_chain_snapshot_mbp(trade_date: str, mbp_row: int, contracts: list[str] | None = None) -> dict[str, dict[str, Any]]:
    idx_path = STAGE6_DIR / "futures" / trade_date / "fut_mbp_opt_mbp_idx.parquet"
    base_dir = STAGE6_DIR / "options" / trade_date / "mbp"
    if not idx_path.exists() or not base_dir.exists():
        return {}
    idx_df = pl.read_parquet(idx_path)
    row = idx_df.filter(pl.col("mbp_row") == mbp_row)
    if row.is_empty():
        return {}
    selected = contracts or [col.removeprefix("last_opt_mbp_row__") for col in idx_df.columns if col.startswith("last_opt_mbp_row__")]
    out: dict[str, dict[str, Any]] = {}
    for contract_key in selected:
        col = f"last_opt_mbp_row__{contract_key}"
        if col not in row.columns:
            continue
        opt_row = int(row[col][0])
        if opt_row < 0:
            continue
        path = base_dir / f"{_safe_contract_filename(contract_key)}.parquet"
        if not path.exists():
            continue
        contract_df = pl.read_parquet(path)
        event = contract_df.filter(pl.col("opt_mbp_row") == opt_row)
        if not event.is_empty():
            out[contract_key] = event.row(0, named=True)
    return out


def get_chain_snapshot_trade(trade_date: str, mbp_row: int, contracts: list[str] | None = None) -> dict[str, dict[str, Any]]:
    idx_path = STAGE6_DIR / "futures" / trade_date / "fut_mbp_opt_trade_idx.parquet"
    base_dir = STAGE6_DIR / "options" / trade_date / "trade"
    if not idx_path.exists() or not base_dir.exists():
        return {}
    idx_df = pl.read_parquet(idx_path)
    row = idx_df.filter(pl.col("mbp_row") == mbp_row)
    if row.is_empty():
        return {}
    selected = contracts or [col.removeprefix("last_opt_trade_row__") for col in idx_df.columns if col.startswith("last_opt_trade_row__")]
    out: dict[str, dict[str, Any]] = {}
    for contract_key in selected:
        col = f"last_opt_trade_row__{contract_key}"
        if col not in row.columns:
            continue
        opt_row = int(row[col][0])
        if opt_row < 0:
            continue
        path = base_dir / f"{_safe_contract_filename(contract_key)}.parquet"
        if not path.exists():
            continue
        contract_df = pl.read_parquet(path)
        event = contract_df.filter(pl.col("opt_trade_row") == opt_row)
        if not event.is_empty():
            out[contract_key] = event.row(0, named=True)
    return out


def get_contract_mbp_history(trade_date: str, contract_key: str, mbp_row_start: int, mbp_row_end: int) -> pl.DataFrame:
    path = STAGE6_DIR / "options" / trade_date / "mbp" / f"{_safe_contract_filename(contract_key)}.parquet"
    if not path.exists():
        return pl.DataFrame()
    df = pl.read_parquet(path)
    return df.filter(pl.col("opt_mbp_row").is_between(mbp_row_start, mbp_row_end, closed="both"))


def get_contract_trade_history(trade_date: str, contract_key: str, mbp_row_start: int, mbp_row_end: int) -> pl.DataFrame:
    path = STAGE6_DIR / "options" / trade_date / "trade" / f"{_safe_contract_filename(contract_key)}.parquet"
    if not path.exists():
        return pl.DataFrame()
    df = pl.read_parquet(path)
    return df.filter(pl.col("opt_trade_row").is_between(mbp_row_start, mbp_row_end, closed="both"))


def build_future_anchor_indices(
    trade_date: str,
    filters: UniverseFilters | None = None,
    column_overrides: Stage6ColumnOverrides | None = None,
) -> dict[str, Any]:
    stage_start_time = time.perf_counter()
    filters = filters or UniverseFilters()
    column_overrides = column_overrides or Stage6ColumnOverrides()

    fut_trade_path = _stage_path(STAGE4_DIR, "trades", "und", trade_date, "clean_trades.parquet")
    fut_mbp_path = _stage_path(STAGE5_DIR, "mbp-10", "und", trade_date, "clean_mbp10.parquet")
    opt_trade_path = _stage_path(STAGE4_DIR, "trades", "opt", trade_date, "clean_trades.parquet")
    opt_mbp_path = _stage_path(STAGE5_DIR, "mbp-10", "opt", trade_date, "clean_mbp10.parquet")

    if not fut_trade_path.exists() or not fut_mbp_path.exists():
        logger.warning("Skipping future-anchor outputs for %s: missing und stage4/stage5 input", trade_date)
        return {"trade_date": trade_date, "skipped": True}

    futures_dir = STAGE6_DIR / "futures" / trade_date
    options_dir = STAGE6_DIR / "options" / trade_date
    futures_dir.mkdir(parents=True, exist_ok=True)
    (options_dir / "mbp").mkdir(parents=True, exist_ok=True)
    (options_dir / "trade").mkdir(parents=True, exist_ok=True)
    _clear_parquet_files(options_dir / "mbp")
    _clear_parquet_files(options_dir / "trade")

    fut_trade_sample = read_parquet_with_schema(
        fut_trade_path,
        "fut_trade",
        extra_cols=column_overrides.fut_trade_extra_cols,
        drop_cols=column_overrides.fut_trade_drop_cols,
        n_rows=1,
    )
    fut_trade_df = read_parquet_with_schema(
        fut_trade_path,
        "fut_trade",
        extra_cols=column_overrides.fut_trade_extra_cols,
        drop_cols=column_overrides.fut_trade_drop_cols,
    ).sort(
        _sort_columns(fut_trade_sample, include_contract=False)
    )
    fut_trade_df = _add_global_row(fut_trade_df, "fut_trade_row")

    fut_mbp_sample = read_parquet_with_schema(
        fut_mbp_path,
        "fut_mbp",
        extra_cols=column_overrides.fut_mbp_extra_cols,
        drop_cols=column_overrides.fut_mbp_drop_cols,
        n_rows=1,
    )
    fut_mbp_df = read_parquet_with_schema(
        fut_mbp_path,
        "fut_mbp",
        extra_cols=column_overrides.fut_mbp_extra_cols,
        drop_cols=column_overrides.fut_mbp_drop_cols,
    ).sort(
        _sort_columns(fut_mbp_sample, include_contract=False)
    )
    fut_mbp_df = _add_global_row(fut_mbp_df, "mbp_row")

    fut_trade_ts = _datetime_ns_array(fut_trade_df["ts_recv_ns"])
    fut_mbp_ts = _datetime_ns_array(fut_mbp_df["ts_recv_ns"])
    fut_mbp_df = fut_mbp_df.with_columns(
        pl.Series("last_fut_trade_row", _searchsorted_last(fut_trade_ts, fut_mbp_ts))
    )

    opt_trade_df = (
        read_parquet_with_schema(
            opt_trade_path,
            "opt_trade",
            extra_cols=column_overrides.opt_trade_extra_cols,
            drop_cols=column_overrides.opt_trade_drop_cols,
        )
        if opt_trade_path.exists() else pl.DataFrame()
    )
    universe_df = build_contract_universe(trade_date, fut_mbp_df, opt_trade_df, filters)
    universe_df.write_csv(futures_dir / "contracts_universe.csv")
    universe_contracts = universe_df["contract_key"].to_list() if not universe_df.is_empty() else []

    opt_trade_filtered = pl.DataFrame()
    opt_mbp_filtered = pl.DataFrame()
    if universe_contracts:
        opt_trade_filtered = _load_frame(
            opt_trade_path,
            "opt_trade",
            filter_contracts=universe_contracts,
            extra_cols=column_overrides.opt_trade_extra_cols,
            drop_cols=column_overrides.opt_trade_drop_cols,
        )
        opt_mbp_filtered = _load_frame(
            opt_mbp_path,
            "opt_mbp",
            filter_contracts=universe_contracts,
            extra_cols=column_overrides.opt_mbp_extra_cols,
            drop_cols=column_overrides.opt_mbp_drop_cols,
        )

        if not opt_trade_filtered.is_empty():
            opt_trade_filtered = opt_trade_filtered.sort(_sort_columns(opt_trade_filtered, include_contract=True))
            opt_trade_filtered = _add_group_row(opt_trade_filtered, "opt_trade_row")
            _write_option_contract_files(opt_trade_filtered, options_dir / "trade")

        if not opt_mbp_filtered.is_empty():
            opt_mbp_filtered = opt_mbp_filtered.sort(_sort_columns(opt_mbp_filtered, include_contract=True))
            opt_mbp_filtered = _add_group_row(opt_mbp_filtered, "opt_mbp_row")
            _write_option_contract_files(opt_mbp_filtered, options_dir / "mbp")

    chain_mbp = _build_chain_table(
        opt_mbp_filtered,
        row_col="opt_mbp_row",
        chain_row_col="chain_mbp_row",
        select_cols=[
            col for col in [
                "ts_recv_ns", "contract_key", "symbol", "underlying_symbol",
                "opt_mbp_row", "best_bid_px", "best_ask_px", "best_bid_sz", "best_ask_sz",
                "book_quality_bucket",
            ] if col in opt_mbp_filtered.columns
        ],
    )
    chain_trade = _build_chain_table(
        opt_trade_filtered,
        row_col="opt_trade_row",
        chain_row_col="chain_trade_row",
        select_cols=[
            col for col in [
                "ts_recv_ns", "contract_key", "symbol", "underlying_symbol",
                "opt_trade_row", "price", "size", "trade_side", "quality_bucket",
            ] if col in opt_trade_filtered.columns
        ],
    )

    chain_mbp.write_parquet(options_dir / "option_chain_mbp.parquet", compression="zstd")
    chain_trade.write_parquet(options_dir / "option_chain_trade.parquet", compression="zstd")

    chain_mbp_idx = (
        _searchsorted_last(_datetime_ns_array(chain_mbp["ts_ns"]), fut_mbp_ts)
        if not chain_mbp.is_empty()
        else np.full(len(fut_mbp_df), -1, dtype=np.int64)
    )
    chain_trade_idx = (
        _searchsorted_last(_datetime_ns_array(chain_trade["ts_ns"]), fut_mbp_ts)
        if not chain_trade.is_empty()
        else np.full(len(fut_mbp_df), -1, dtype=np.int64)
    )

    fut_mbp_df = fut_mbp_df.with_columns([
        pl.Series("last_chain_mbp_row", chain_mbp_idx),
        pl.Series("last_chain_trade_row", chain_trade_idx),
    ])

    opt_mbp_idx_df = _build_per_contract_idx(
        fut_mbp_df=fut_mbp_df,
        option_df=opt_mbp_filtered,
        universe=universe_df,
        prefix="last_opt_mbp_row__",
    )
    opt_trade_idx_df = _build_per_contract_idx(
        fut_mbp_df=fut_mbp_df,
        option_df=opt_trade_filtered,
        universe=universe_df,
        prefix="last_opt_trade_row__",
    )

    fut_trade_df.write_parquet(futures_dir / "trade.parquet", compression="zstd")
    fut_mbp_df.write_parquet(futures_dir / "mbp.parquet", compression="zstd")
    opt_mbp_idx_df.write_parquet(futures_dir / "fut_mbp_opt_mbp_idx.parquet", compression="zstd")
    opt_trade_idx_df.write_parquet(futures_dir / "fut_mbp_opt_trade_idx.parquet", compression="zstd")

    validation = validate_future_anchor_outputs(trade_date)
    logger.info(
        "Finished future-anchor Stage 6 for %s | fut_mbp_rows=%s fut_trade_rows=%s universe=%s chain_mbp_rows=%s chain_trade_rows=%s total_time=%.2f seconds",
        trade_date,
        len(fut_mbp_df),
        len(fut_trade_df),
        len(universe_df),
        len(chain_mbp),
        len(chain_trade),
        time.perf_counter() - stage_start_time,
    )
    logger.info("Stage 6 future-anchor validation for %s: %s", trade_date, validation)
    return validation


def validate_future_anchor_outputs(trade_date: str) -> dict[str, Any]:
    futures_dir = STAGE6_DIR / "futures" / trade_date
    options_dir = STAGE6_DIR / "options" / trade_date

    fut_mbp = pl.read_parquet(futures_dir / "mbp.parquet")
    fut_trade = pl.read_parquet(futures_dir / "trade.parquet")
    idx_mbp = pl.read_parquet(futures_dir / "fut_mbp_opt_mbp_idx.parquet")
    idx_trade = pl.read_parquet(futures_dir / "fut_mbp_opt_trade_idx.parquet")
    chain_mbp = pl.read_parquet(options_dir / "option_chain_mbp.parquet")
    chain_trade = pl.read_parquet(options_dir / "option_chain_trade.parquet")

    result: dict[str, Any] = {
        "trade_date": trade_date,
        "fut_mbp_row_ok": fut_mbp["mbp_row"].to_list() == list(range(len(fut_mbp))),
        "fut_trade_row_ok": fut_trade["fut_trade_row"].to_list() == list(range(len(fut_trade))),
        "last_fut_trade_row_monotonic": _validate_monotonic(fut_mbp["last_fut_trade_row"]),
        "last_chain_mbp_row_monotonic": _validate_monotonic(fut_mbp["last_chain_mbp_row"]),
        "last_chain_trade_row_monotonic": _validate_monotonic(fut_mbp["last_chain_trade_row"]),
        "opt_mbp_idx_columns": len([c for c in idx_mbp.columns if c.startswith("last_opt_mbp_row__")]),
        "opt_trade_idx_columns": len([c for c in idx_trade.columns if c.startswith("last_opt_trade_row__")]),
    }

    sample_rows = [0, len(fut_mbp) // 2, len(fut_mbp) - 1] if len(fut_mbp) else []
    sample_rows = sorted(set([row for row in sample_rows if row >= 0]))
    mbp_consistency = True
    trade_consistency = True

    mbp_columns = [c for c in idx_mbp.columns if c.startswith("last_opt_mbp_row__")]
    trade_columns = [c for c in idx_trade.columns if c.startswith("last_opt_trade_row__")]

    for row_idx in sample_rows:
        if mbp_columns and int(fut_mbp["last_chain_mbp_row"][row_idx]) >= 0:
            per_contract_times: list[int] = []
            for col in mbp_columns:
                contract_key = col.removeprefix("last_opt_mbp_row__")
                local_row = int(idx_mbp[col][row_idx])
                if local_row < 0:
                    continue
                contract_df = pl.read_parquet(options_dir / "mbp" / f"{_safe_contract_filename(contract_key)}.parquet")
                event = contract_df.filter(pl.col("opt_mbp_row") == local_row)
                if not event.is_empty():
                    per_contract_times.append(int(event["ts_recv_ns"].dt.epoch("ns")[0]))
            if per_contract_times:
                chain_ts = int(chain_mbp["ts_ns"].dt.epoch("ns")[int(fut_mbp["last_chain_mbp_row"][row_idx])])
                mbp_consistency = mbp_consistency and (max(per_contract_times) == chain_ts)

        if trade_columns and int(fut_mbp["last_chain_trade_row"][row_idx]) >= 0:
            per_contract_times = []
            for col in trade_columns:
                contract_key = col.removeprefix("last_opt_trade_row__")
                local_row = int(idx_trade[col][row_idx])
                if local_row < 0:
                    continue
                contract_df = pl.read_parquet(options_dir / "trade" / f"{_safe_contract_filename(contract_key)}.parquet")
                event = contract_df.filter(pl.col("opt_trade_row") == local_row)
                if not event.is_empty():
                    per_contract_times.append(int(event["ts_recv_ns"].dt.epoch("ns")[0]))
            if per_contract_times:
                chain_ts = int(chain_trade["ts_ns"].dt.epoch("ns")[int(fut_mbp["last_chain_trade_row"][row_idx])])
                trade_consistency = trade_consistency and (max(per_contract_times) == chain_ts)

    result["sample_chain_mbp_consistent"] = mbp_consistency
    result["sample_chain_trade_consistent"] = trade_consistency
    return result


def align_trade_book(trade_date: str, asset_type: str) -> None:
    """Legacy Stage 6: 将 Trade 和最近的 Book Snapshot 做 join_asof，对下游 Stage7 保持兼容。"""
    stage_start_time = time.perf_counter()
    trade_dir = get_processed_path(STAGE4_DIR, asset_type, "trades", trade_date)
    book_dir = get_processed_path(STAGE5_DIR, asset_type, "mbp-10", trade_date)

    trade_path = trade_dir / "clean_trades.parquet"
    book_path = book_dir / "clean_mbp10.parquet"

    if not trade_path.exists():
        logger.warning("Skipping legacy alignment for %s - %s: missing %s", trade_date, asset_type, trade_path)
        return
    if not book_path.exists():
        logger.warning("Skipping legacy alignment for %s - %s: missing %s", trade_date, asset_type, book_path)
        return

    trades_lf = pl.scan_parquet(trade_dir / "*.parquet")
    book_lf = (
        pl.scan_parquet(book_dir / "*.parquet")
        .select([
            "contract_key",
            "ts_recv_ns",
            "best_bid_px",
            "best_ask_px",
            "best_bid_sz",
            "best_ask_sz",
            "mid_px",
            "spread",
            "book_quality_bucket",
        ])
        .rename({"ts_recv_ns": "book_ts_recv_ns"})
    )

    aligned_lf = trades_lf.join_asof(
        book_lf,
        left_on="ts_recv_ns",
        right_on="book_ts_recv_ns",
        by=["contract_key"],
        strategy="backward",
        tolerance=LEGACY_ALIGNMENT_TOLERANCE_NS,
    )

    aligned_lf = (
        aligned_lf.with_columns([
            pl.col("book_ts_recv_ns").is_not_null().alias("has_aligned_book"),
            (pl.col("ts_recv_ns") - pl.col("book_ts_recv_ns")).alias("book_age_ns"),
            pl.when(pl.col("side") == "B").then(pl.lit(1))
            .when(pl.col("side") == "A").then(pl.lit(-1))
            .otherwise(pl.lit(0))
            .alias("db_sign"),
            pl.when(pl.col("price") > pl.col("mid_px")).then(pl.lit(1))
            .when(pl.col("price") < pl.col("mid_px")).then(pl.lit(-1))
            .otherwise(pl.lit(0))
            .alias("qr_sign"),
        ])
        .with_columns([
            pl.when(pl.col("side") != "N").then(pl.col("db_sign")).otherwise(pl.col("qr_sign")).alias("final_sign"),
            pl.when(pl.col("mid_px").is_not_null())
            .then((pl.col("price") - pl.col("mid_px")) / pl.col("mid_px") * 10000)
            .otherwise(pl.lit(None))
            .alias("trade_vs_mid_bp"),
            pl.col("price").alias("price_f"),
            pl.col("best_bid_px").alias("bid_f"),
            pl.col("best_ask_px").alias("ask_f"),
        ])
    )

    out_dir = get_processed_path(STAGE6_DIR, asset_type, "aligned", trade_date)
    out_path = out_dir / "aligned_trade_book.parquet"
    aligned_lf.collect(engine="streaming").write_parquet(out_path, compression="zstd")
    logger.info(
        "Finished legacy alignment for %s - %s -> %s | total_time=%.2f seconds | output_size=%.2f MB",
        trade_date,
        asset_type,
        out_path,
        time.perf_counter() - stage_start_time,
        out_path.stat().st_size / (1024 * 1024),
    )


def run_stage6_align(
    trade_dates: list[str],
    filters: UniverseFilters | None = None,
    column_overrides: Stage6ColumnOverrides | None = None,
) -> None:
    for trade_date in trade_dates:
        build_future_anchor_indices(trade_date, filters=filters, column_overrides=column_overrides)
        for asset_type in ["und", "opt"]:
            align_trade_book(trade_date, asset_type)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Stage 6 future-anchor indexing and legacy alignment.")
    parser.add_argument("dates", nargs="*", default=["2026-05-24"], help="Trade dates to process, e.g. 2026-05-24")
    parser.add_argument("--min-trade-volume", type=int, default=DEFAULT_MIN_TRADE_VOLUME)
    parser.add_argument("--min-trade-notional", type=float, default=0.0)
    parser.add_argument("--call-put", choices=["C", "P"])
    parser.add_argument("--expiry-start")
    parser.add_argument("--expiry-end")
    parser.add_argument("--atm-top-n", type=int)
    parser.add_argument("--max-contracts", type=int)
    parser.add_argument("--contract-list", nargs="*")
    parser.add_argument("--fut-mbp-extra-cols", nargs="*")
    parser.add_argument("--fut-mbp-drop-cols", nargs="*")
    parser.add_argument("--opt-mbp-extra-cols", nargs="*")
    parser.add_argument("--opt-mbp-drop-cols", nargs="*")
    parser.add_argument("--fut-trade-extra-cols", nargs="*")
    parser.add_argument("--fut-trade-drop-cols", nargs="*")
    parser.add_argument("--opt-trade-extra-cols", nargs="*")
    parser.add_argument("--opt-trade-drop-cols", nargs="*")
    args = parser.parse_args()

    filters = UniverseFilters(
        min_trade_volume=args.min_trade_volume,
        min_trade_notional=args.min_trade_notional,
        call_put=args.call_put,
        expiry_start=args.expiry_start,
        expiry_end=args.expiry_end,
        atm_top_n=args.atm_top_n,
        max_contracts=args.max_contracts,
        contract_list=args.contract_list,
    )
    column_overrides = Stage6ColumnOverrides(
        fut_mbp_extra_cols=args.fut_mbp_extra_cols,
        fut_mbp_drop_cols=args.fut_mbp_drop_cols,
        opt_mbp_extra_cols=args.opt_mbp_extra_cols,
        opt_mbp_drop_cols=args.opt_mbp_drop_cols,
        fut_trade_extra_cols=args.fut_trade_extra_cols,
        fut_trade_drop_cols=args.fut_trade_drop_cols,
        opt_trade_extra_cols=args.opt_trade_extra_cols,
        opt_trade_drop_cols=args.opt_trade_drop_cols,
    )
    run_stage6_align(args.dates, filters=filters, column_overrides=column_overrides)
