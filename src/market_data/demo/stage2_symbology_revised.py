import argparse
import logging

import polars as pl

from config import PROCESSED_ROOT
from stage2_symbology import collect_observed_instruments, load_filtered_definition_mapping

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STAGE2_REVISED_DIR = PROCESSED_ROOT / 'stage2_symbology_revised'
STAGE2_REVISED_DAILY_DIR = STAGE2_REVISED_DIR / 'daily'
STAGE2_REVISED_HISTORY_DIR = STAGE2_REVISED_DIR / 'history'
REVISED_STAGE_VERSION = 'v2.0-revised'

IDENTIFIER_COLS = ['asset_type', 'publisher_id', 'instrument_id']
VALUE_COLS = [
    'symbol',
    'contract_key',
    'contract_type',
    'underlying_contract_key',
    'underlying_symbol',
    'expiry_date',
    'multiplier',
    'option_type',
    'strike',
]


def build_daily_symbology_frames(trade_date: str) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
    observed_lf = collect_observed_instruments(trade_date)
    if observed_lf is None:
        return None, None

    schema_defaults = [
        pl.lit(None, dtype=pl.Utf8).alias('def_symbol'),
        pl.lit(None, dtype=pl.Utf8).alias('instrument_class'),
        pl.lit(None, dtype=pl.Utf8).alias('underlying_contract_key'),
        pl.lit(None, dtype=pl.Utf8).alias('underlying_symbol'),
        pl.lit(None, dtype=pl.Date).alias('expiry_date'),
        pl.lit(None, dtype=pl.Float64).alias('multiplier'),
        pl.lit(None, dtype=pl.Float64).alias('strike'),
    ]

    joined_frames: list[pl.LazyFrame] = []
    unresolved_frames: list[pl.LazyFrame] = []

    for asset_type in ['und', 'opt']:
        asset_observed_lf = observed_lf.filter(pl.col('observed_asset_type') == asset_type)
        definition_lf, has_filtered_definition = load_filtered_definition_mapping(trade_date, asset_type)

        if definition_lf is not None:
            asset_joined_lf = asset_observed_lf.join(
                definition_lf,
                on=['publisher_id', 'instrument_id'],
                how='left',
            )
        else:
            asset_joined_lf = asset_observed_lf.with_columns(schema_defaults)

        joined_frames.append(asset_joined_lf)
        unresolved_frames.append(
            asset_joined_lf
            .filter(pl.col('def_symbol').is_null())
            .select([
                pl.lit(trade_date).cast(pl.Date).alias('trade_date'),
                pl.lit(asset_type).alias('asset_type'),
                pl.col('observed_symbol').alias('symbol'),
                'publisher_id',
                'instrument_id',
                pl.when(pl.lit(has_filtered_definition))
                .then(pl.lit(f'missing_{asset_type}_definition_match'))
                .otherwise(pl.lit(f'{asset_type}_definition_file_missing'))
                .alias('unresolved_reason'),
                pl.lit(REVISED_STAGE_VERSION).alias('stage_version'),
            ])
        )

    joined_lf = pl.concat(joined_frames, how='diagonal_relaxed')
    unresolved_lf = pl.concat(unresolved_frames, how='diagonal_relaxed').unique(subset=['publisher_id', 'instrument_id'])

    sym_df = (
        joined_lf
        .with_columns([
            pl.lit(trade_date).cast(pl.Date).alias('trade_date'),
            pl.col('observed_asset_type').alias('asset_type'),
            pl.coalesce(['observed_symbol', 'def_symbol']).alias('symbol'),
            pl.coalesce(['def_symbol', 'observed_symbol']).alias('contract_key'),
            pl.when(pl.col('observed_asset_type') == 'opt')
            .then(pl.lit('OPT'))
            .otherwise(pl.lit('FUT'))
            .alias('contract_type'),
            pl.when(pl.col('observed_asset_type') == 'opt')
            .then(pl.col('instrument_class'))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias('option_type'),
            pl.lit(REVISED_STAGE_VERSION).alias('stage_version'),
        ])
        .select([
            'trade_date',
            'asset_type',
            'symbol',
            'publisher_id',
            'instrument_id',
            'contract_key',
            'contract_type',
            'underlying_contract_key',
            'underlying_symbol',
            'expiry_date',
            'multiplier',
            'option_type',
            'strike',
            'stage_version',
        ])
        .unique(subset=['publisher_id', 'instrument_id'])
        .collect()
    )

    unresolved_df = unresolved_lf.collect()
    return sym_df, unresolved_df


def write_daily_snapshot(trade_date: str, sym_df: pl.DataFrame, unresolved_df: pl.DataFrame) -> None:
    out_dir = STAGE2_REVISED_DAILY_DIR / trade_date
    out_dir.mkdir(parents=True, exist_ok=True)
    sym_path = out_dir / 'symbology.parquet'
    unresolved_path = out_dir / 'symbology_unresolved.parquet'
    sym_df.write_parquet(sym_path, compression='zstd')
    unresolved_df.write_parquet(unresolved_path, compression='zstd')
    logger.info('Saved revised daily symbology to %s', sym_path)
    logger.info('Saved revised daily unresolved symbology rows to %s', unresolved_path)


def build_history_artifacts(sym_snapshots: list[pl.DataFrame]) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    all_snapshots = (
        pl.concat(sym_snapshots, how='diagonal_relaxed')
        .sort(IDENTIFIER_COLS + ['trade_date'])
        .with_columns(pl.struct(VALUE_COLS).hash().alias('state_hash'))
        .with_columns([
            pl.col('state_hash').shift(1).over(IDENTIFIER_COLS).alias('prev_state_hash'),
        ])
        .with_columns([
            (
                pl.col('prev_state_hash').is_null()
                | (pl.col('state_hash') != pl.col('prev_state_hash'))
            ).alias('is_version_start')
        ])
        .with_columns([
            pl.col('is_version_start').cast(pl.Int32).cum_sum().over(IDENTIFIER_COLS).alias('version_seq'),
        ])
        .with_columns([
            pl.concat_str([
                pl.col('asset_type'),
                pl.col('publisher_id').cast(pl.Utf8),
                pl.col('instrument_id').cast(pl.Utf8),
                pl.col('version_seq').cast(pl.Utf8),
            ], separator='|').alias('version_id')
        ])
    )

    date_index_df = (
        all_snapshots
        .select([
            'trade_date',
            *IDENTIFIER_COLS,
            'version_id',
            'version_seq',
            'symbol',
            'contract_key',
            'underlying_symbol',
            'is_version_start',
        ])
        .sort(IDENTIFIER_COLS + ['trade_date'])
    )

    version_df = (
        all_snapshots
        .group_by(IDENTIFIER_COLS + ['version_seq', 'version_id'])
        .agg([
            pl.col('trade_date').min().alias('effective_from_date'),
            pl.col('trade_date').max().alias('observed_end_date'),
            pl.len().alias('observed_trade_dates_count'),
            *[pl.col(col).first().alias(col) for col in VALUE_COLS],
        ])
        .sort(IDENTIFIER_COLS + ['version_seq'])
        .with_columns([
            pl.col('effective_from_date').shift(-1).over(IDENTIFIER_COLS).alias('effective_to_date_exclusive'),
        ])
        .with_columns([
            pl.col('effective_to_date_exclusive').is_null().alias('is_current_version'),
            pl.lit(REVISED_STAGE_VERSION).alias('stage_version'),
        ])
        .select([
            'version_id',
            'version_seq',
            *IDENTIFIER_COLS,
            *VALUE_COLS,
            'effective_from_date',
            'effective_to_date_exclusive',
            'observed_end_date',
            'observed_trade_dates_count',
            'is_current_version',
            'stage_version',
        ])
    )

    change_events_df = (
        date_index_df
        .filter(pl.col('is_version_start'))
        .select([
            'trade_date',
            *IDENTIFIER_COLS,
            'version_id',
            'version_seq',
        ])
        .join(
            version_df.select(['version_id', *VALUE_COLS, 'effective_from_date', 'effective_to_date_exclusive']),
            on='version_id',
            how='left',
        )
        .sort(IDENTIFIER_COLS + ['trade_date'])
    )

    return version_df, date_index_df, change_events_df


def write_history_artifacts(version_df: pl.DataFrame, date_index_df: pl.DataFrame, change_events_df: pl.DataFrame) -> None:
    STAGE2_REVISED_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    version_path = STAGE2_REVISED_HISTORY_DIR / 'symbology_versions.parquet'
    date_index_path = STAGE2_REVISED_HISTORY_DIR / 'symbology_date_index.parquet'
    change_events_path = STAGE2_REVISED_HISTORY_DIR / 'symbology_change_events.parquet'

    version_df.write_parquet(version_path, compression='zstd')
    date_index_df.write_parquet(date_index_path, compression='zstd')
    change_events_df.write_parquet(change_events_path, compression='zstd')

    logger.info('Saved revised symbology versions to %s', version_path)
    logger.info('Saved revised symbology date index to %s', date_index_path)
    logger.info('Saved revised symbology change events to %s', change_events_path)


def run_stage2_symbology_revised(trade_dates: list[str]) -> None:
    logger.info('--- Starting Stage 2 Revised Symbology Generation for %s ---', trade_dates)
    sym_snapshots: list[pl.DataFrame] = []

    for trade_date in sorted(set(trade_dates)):
        sym_df, unresolved_df = build_daily_symbology_frames(trade_date)
        if sym_df is None or unresolved_df is None:
            continue

        write_daily_snapshot(trade_date, sym_df, unresolved_df)
        sym_snapshots.append(sym_df)

    if not sym_snapshots:
        logger.warning('No revised symbology snapshots were produced.')
        return

    version_df, date_index_df, change_events_df = build_history_artifacts(sym_snapshots)
    write_history_artifacts(version_df, date_index_df, change_events_df)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build revised Stage 2 symbology outputs without modifying the legacy snapshot layout.')
    parser.add_argument('dates', nargs='*', help='Space separated trade dates, e.g. 2026-05-21 2026-05-22')
    parser.add_argument('--dates', dest='dates_csv', default='', help='Comma separated trade dates, e.g. 2026-05-21,2026-05-22')
    args = parser.parse_args()

    if args.dates:
        dates = args.dates
    elif args.dates_csv:
        dates = [item.strip() for item in args.dates_csv.split(',') if item.strip()]
    else:
        dates = ['2026-05-22']

    run_stage2_symbology_revised(dates)