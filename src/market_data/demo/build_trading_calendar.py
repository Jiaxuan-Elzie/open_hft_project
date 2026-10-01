from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
import polars as pl

from config import PRODUCT_CALENDAR_MAP, TRADING_CALENDAR_DIR, resolve_calendar_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Build trading calendar reference tables for stage3.')
    parser.add_argument('--start-date', required=True, help='Start trade date, for example 2026-01-01')
    parser.add_argument('--end-date', required=True, help='End trade date, for example 2026-12-31')
    parser.add_argument(
        '--products',
        nargs='*',
        default=sorted(PRODUCT_CALENDAR_MAP.keys()),
        help='Product roots to build, default is all configured products.',
    )
    parser.add_argument(
        '--output-dir',
        default=str(TRADING_CALENDAR_DIR),
        help='Directory where parquet outputs will be written.',
    )
    return parser.parse_args()


def build_session_rows(product_root: str, start_date: str, end_date: str) -> list[dict]:
    config = resolve_calendar_config(product_root)
    calendar_name = config['calendar_name']
    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(start_date=start_date, end_date=end_date)

    rows: list[dict] = []
    for trade_date, record in schedule.iterrows():
        rows.append(
            {
                'product_root': product_root,
                'calendar_name': calendar_name,
                'exchange': config['exchange'],
                'session_type': config['session_type'],
                'trade_date': pd.Timestamp(trade_date).date(),
                'session_open_utc': pd.Timestamp(record['market_open']).tz_convert('UTC'),
                'session_close_utc': pd.Timestamp(record['market_close']).tz_convert('UTC'),
                'source': 'pandas_market_calendars',
            }
        )
    return rows


def split_session_to_utc_days(session_row: dict) -> list[dict]:
    open_ts = pd.Timestamp(session_row['session_open_utc']).tz_convert('UTC')
    close_ts = pd.Timestamp(session_row['session_close_utc']).tz_convert('UTC')

    rows: list[dict] = []
    cursor = open_ts
    segment_index = 0

    while cursor < close_ts:
        next_midnight = cursor.normalize() + pd.Timedelta(days=1)
        segment_end = min(close_ts, next_midnight)
        rows.append(
            {
                'product_root': session_row['product_root'],
                'calendar_name': session_row['calendar_name'],
                'exchange': session_row['exchange'],
                'session_type': session_row['session_type'],
                'utc_date': cursor.date(),
                'calendar_trade_date': session_row['trade_date'],
                'interval_start_utc': cursor,
                'interval_end_utc': segment_end,
                'segment_index': segment_index,
                'is_rth': True,
                'source': session_row['source'],
            }
        )
        cursor = segment_end
        segment_index += 1

    return rows


def build_trading_calendar(start_date: str, end_date: str, products: list[str], output_dir: Path) -> tuple[Path, Path]:
    session_rows: list[dict] = []
    for product_root in products:
        logger.info('Building trading calendar for %s', product_root)
        session_rows.extend(build_session_rows(product_root, start_date, end_date))

    if not session_rows:
        raise ValueError('No trading calendar rows were generated.')

    interval_rows: list[dict] = []
    for row in session_rows:
        interval_rows.extend(split_session_to_utc_days(row))

    output_dir.mkdir(parents=True, exist_ok=True)
    sessions_path = output_dir / 'session_calendar.parquet'
    intervals_path = output_dir / 'session_calendar_by_utc_day.parquet'

    sessions_df = pl.from_pandas(pd.DataFrame(session_rows))
    intervals_df = pl.from_pandas(pd.DataFrame(interval_rows))

    sessions_df.write_parquet(sessions_path, compression='zstd')
    intervals_df.write_parquet(intervals_path, compression='zstd')

    logger.info('Saved session calendar to %s', sessions_path)
    logger.info('Saved UTC day calendar to %s', intervals_path)
    return sessions_path, intervals_path


def main() -> None:
    args = parse_args()
    build_trading_calendar(
        start_date=args.start_date,
        end_date=args.end_date,
        products=args.products,
        output_dir=Path(args.output_dir),
    )


if __name__ == '__main__':
    main()
