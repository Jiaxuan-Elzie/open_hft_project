import logging
import polars as pl

from config import DEF_FILTER_FUT_DIR, DEF_FILTER_OPT_DIR, STAGE1_DIR, STAGE2_DIR, get_processed_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def collect_observed_instruments(trade_date: str) -> pl.LazyFrame | None:
    """Collect instrument ids actually observed in stage1 outputs."""
    observed_frames: list[pl.LazyFrame] = []

    for asset_type in ["und", "opt"]:
        for schema in ["trades", "mbp-10"]:
            in_dir = get_processed_path(STAGE1_DIR, asset_type, schema, trade_date)
            if not in_dir.exists():
                continue

            # Check if there are actually parquet files in the directory
            if not any(in_dir.glob("*.parquet")):
                continue

            observed_frames.append(
                pl.scan_parquet(in_dir / "*.parquet")
                .select([
                    pl.lit(asset_type).alias("observed_asset_type"),
                    pl.col("publisher_id").cast(pl.Int32).alias("publisher_id"),
                    pl.col("instrument_id").cast(pl.Int64).alias("instrument_id"),
                    pl.col("symbol").cast(pl.Utf8).alias("observed_symbol"),
                ])
            )

    if not observed_frames:
        logger.warning(f"No Stage1 data found for {trade_date}, skipping Symbology build.")
        return None

    return (
        pl.concat(observed_frames, how="diagonal_relaxed")
        .group_by(["publisher_id", "instrument_id"])
        .agg([
            pl.col("observed_asset_type").first(),
            pl.col("observed_symbol").first(),
        ])
    )


def load_filtered_definition_mapping(trade_date: str, asset_type: str) -> tuple[pl.LazyFrame | None, bool]:
    """Load filtered definition mapping from asset-specific definition_filtered outputs."""
    y, m, _ = trade_date.split('-')
    if asset_type == "opt":
        parquet_path = DEF_FILTER_OPT_DIR / y / m / f"{trade_date}.options.parquet"
    elif asset_type == "und":
        parquet_path = DEF_FILTER_FUT_DIR / y / m / f"{trade_date}.futures.parquet"
    else:
        raise ValueError(f"Unsupported asset_type={asset_type}")

    if not parquet_path.exists():
        logger.warning("No filtered %s definition parquet found for %s at %s", asset_type, trade_date, parquet_path)
        return None, False

    logger.info("Building %s symbology for %s from filtered definition %s", asset_type, trade_date, parquet_path.name)

    definition_lf = pl.scan_parquet(parquet_path)
    if asset_type == "opt":
        definition_lf = definition_lf.select([
            pl.col("publisher_id").cast(pl.Int32).alias("publisher_id"),
            pl.col("instrument_id").cast(pl.Int64).alias("instrument_id"),
            pl.col("raw_symbol").cast(pl.Utf8).alias("def_symbol"),
            pl.col("instrument_class").cast(pl.Utf8).alias("instrument_class"),
            pl.col("underlying").cast(pl.Utf8).alias("underlying_contract_key"),
            pl.col("underlying").cast(pl.Utf8).alias("underlying_symbol"),
            pl.col("expiration").cast(pl.Datetime).cast(pl.Date).alias("expiry_date"),
            pl.col("contract_multiplier").cast(pl.Float64).alias("multiplier"),
            pl.col("strike_price").cast(pl.Float64).alias("strike"),
        ])
    else:
        definition_lf = definition_lf.select([
            pl.col("publisher_id").cast(pl.Int32).alias("publisher_id"),
            pl.col("instrument_id").cast(pl.Int64).alias("instrument_id"),
            pl.col("raw_symbol").cast(pl.Utf8).alias("def_symbol"),
            pl.col("instrument_class").cast(pl.Utf8).alias("instrument_class"),
            pl.col("raw_symbol").cast(pl.Utf8).alias("underlying_contract_key"),
            pl.lit(None, dtype=pl.Utf8).alias("underlying_symbol"),
            pl.col("expiration").cast(pl.Datetime).cast(pl.Date).alias("expiry_date"),
            pl.col("contract_multiplier").cast(pl.Float64).alias("multiplier"),
            pl.lit(None, dtype=pl.Float64).alias("strike"),
        ])

    return definition_lf.unique(subset=["publisher_id", "instrument_id"]), True


def build_symbology(trade_date: str) -> None:
    """
    Stage 2: 基于 stage1 实际出现的 instrument 建立统一 symbology。
    und_definition 从 definition_filtered/fut 读取，opt_definition 从 definition_filtered/opt 读取。
    """
    observed_lf = collect_observed_instruments(trade_date)
    if observed_lf is None:
        return

    schema_defaults = [
        pl.lit(None, dtype=pl.Utf8).alias("def_symbol"),
        pl.lit(None, dtype=pl.Utf8).alias("instrument_class"),
        pl.lit(None, dtype=pl.Utf8).alias("underlying_contract_key"),
        pl.lit(None, dtype=pl.Utf8).alias("underlying_symbol"),
        pl.lit(None, dtype=pl.Date).alias("expiry_date"),
        pl.lit(None, dtype=pl.Float64).alias("multiplier"),
        pl.lit(None, dtype=pl.Float64).alias("strike"),
    ]

    joined_frames: list[pl.LazyFrame] = []
    unresolved_frames: list[pl.LazyFrame] = []

    for asset_type in ["und", "opt"]:
        asset_observed_lf = observed_lf.filter(pl.col("observed_asset_type") == asset_type)
        definition_lf, has_filtered_definition = load_filtered_definition_mapping(trade_date, asset_type)

        if definition_lf is not None:
            asset_joined_lf = asset_observed_lf.join(
                definition_lf,
                on=["publisher_id", "instrument_id"],
                how="left",
            )
        else:
            asset_joined_lf = asset_observed_lf.with_columns(schema_defaults)

        joined_frames.append(asset_joined_lf)
        unresolved_frames.append(
            asset_joined_lf
            .filter(pl.col("def_symbol").is_null())
            .select([
                pl.lit(trade_date).cast(pl.Date).alias("trade_date"),
                pl.lit(asset_type).alias("asset_type"),
                pl.col("observed_symbol").alias("symbol"),
                "publisher_id",
                "instrument_id",
                pl.when(pl.lit(has_filtered_definition))
                  .then(pl.lit(f"missing_{asset_type}_definition_match"))
                  .otherwise(pl.lit(f"{asset_type}_definition_file_missing"))
                  .alias("unresolved_reason"),
                pl.lit("v1.2").alias("stage_version"),
            ])
        )

    joined_lf = pl.concat(joined_frames, how="diagonal_relaxed")

    sym_df = (
        joined_lf
        .with_columns([
            pl.lit(trade_date).cast(pl.Date).alias("trade_date"),
            pl.col("observed_asset_type").alias("asset_type"),
            pl.coalesce(["observed_symbol", "def_symbol"]).alias("symbol"),
            pl.coalesce(["def_symbol", "observed_symbol"]).alias("contract_key"),
            pl.when(pl.col("observed_asset_type") == "opt")
              .then(pl.lit("OPT"))
              .otherwise(pl.lit("FUT"))
              .alias("contract_type"),
            pl.when(pl.col("observed_asset_type") == "opt")
              .then(pl.col("instrument_class"))
              .otherwise(pl.lit(None, dtype=pl.Utf8))
              .alias("option_type"),
            pl.lit("v1.2").alias("stage_version"),
        ])
        .select([
            "trade_date",
            "asset_type",
            "symbol",
            "publisher_id",
            "instrument_id",
            "contract_key",
            "contract_type",
            "underlying_contract_key",
            "underlying_symbol",
            "expiry_date",
            "multiplier",
            "option_type",
            "strike",
            "stage_version",
        ])
        .unique(subset=["publisher_id", "instrument_id"])
    )

    unresolved_lf = (
        pl.concat(unresolved_frames, how="diagonal_relaxed")
        .unique(subset=["publisher_id", "instrument_id"])
    )

    out_dir = STAGE2_DIR / trade_date
    out_dir.mkdir(parents=True, exist_ok=True)
    sym_path = out_dir / "symbology.parquet"
    unresolved_path = out_dir / "symbology_unresolved.parquet"

    sym_df.sink_parquet(sym_path, compression="zstd")
    unresolved_lf.sink_parquet(unresolved_path, compression="zstd")
    logger.info("Saved symbology to %s", sym_path)
    logger.info("Saved unresolved symbology rows to %s", unresolved_path)

def run_stage2_symbology(trade_dates: list[str]) -> None:
    logger.info(f"--- Starting Stage 2 Symbology Generation for {trade_dates} ---")
    for d in trade_dates:
        build_symbology(d)

if __name__ == "__main__":
    import sys
    dates = sys.argv[1:] if len(sys.argv) > 1 else ["2026-05-12"]
    run_stage2_symbology(dates)