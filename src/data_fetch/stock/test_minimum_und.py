import argparse
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError
from tqdm import tqdm

# ====== 默认配置 ======
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_STOCK_ROOT = PROJECT_ROOT / "data" / "raw_data" / "stock"
DEFAULT_START_DATE = "2026-05-01"
DEFAULT_END_DATE = "2026-05-14"
DEFAULT_FEED = "quotes"
DEFAULT_MAX_TICKERS = 0
DEFAULT_DOWNLOAD_WORKERS = 4
DEFAULT_CHUNK_SIZE = 1_000_000
DEFAULT_DOWNLOAD_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 3.0
DEFAULT_S3_TRANSFER_CONCURRENCY = 16
DEFAULT_DOWNLOAD_CACHE_DIR = str(RAW_STOCK_ROOT / "cache" / "underlyings")
DEFAULT_DOWNLOAD_READ_CHUNK_BYTES = 16 * 1024 * 1024
DEFAULT_RANGE_SEGMENT_BYTES = 512 * 1024 * 1024
DEFAULT_SP500_CSV_PATH = str(Path(__file__).with_name("sp500_historical_summary.csv"))
DEFAULT_UNDERLYINGS_CSV = DEFAULT_SP500_CSV_PATH
DEFAULT_SP500_ONLY = True
DEFAULT_SKIP_EXISTING = True

ENDPOINT_URL = "https://files.massive.com"
BUCKET_NAME = "flatfiles"

ACCESS_KEY = os.environ.get("MASSIVE_AWS_KEY", "")
SECRET_KEY = os.environ.get("MASSIVE_AWS_SECRET", "")

OUT_DIR = str(RAW_STOCK_ROOT / "underlyings")
# =====================


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read option underlyings and download underlying stock flatfiles via Massive S3 over a date range.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Inclusive start date in ISO format, for example 2026-04-12.")
    parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="Inclusive end date in ISO format, for example 2026-05-12.")
    parser.add_argument(
        "--feed",
        default=DEFAULT_FEED,
        choices=["trades", "quotes"],
        help="Underlying stock flatfile feed to download from S3.",
    )
    parser.add_argument(
        "--underlyings-csv",
        default=DEFAULT_UNDERLYINGS_CSV,
        help="CSV path used as the source ticker list. The file must contain one of: symbol, ticker, underlying_ticker.",
    )
    parser.add_argument("--max-tickers", type=int, default=DEFAULT_MAX_TICKERS, help="Optional cap on number of underlyings to keep after filtering. 0 means all.")
    parser.add_argument("--download-workers", type=int, default=DEFAULT_DOWNLOAD_WORKERS, help="Maximum concurrent day-level flatfile download workers.")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Rows per streamed CSV chunk when filtering stock flatfiles.")
    parser.add_argument("--download-retries", type=int, default=DEFAULT_DOWNLOAD_RETRIES, help="Maximum retry attempts for a single day flatfile download.")
    parser.add_argument("--download-cache-dir", default=DEFAULT_DOWNLOAD_CACHE_DIR, help="Root directory for resumable raw stock flatfile cache files.")
    parser.add_argument("--sp500-csv-path", default=DEFAULT_SP500_CSV_PATH, help="CSV path used to load the S&P 500 symbol universe.")
    parser.add_argument("--sp500-only", dest="sp500_only", action="store_true", default=DEFAULT_SP500_ONLY, help="Keep only underlyings whose symbol exists in the S&P 500 CSV.")
    parser.add_argument("--all-underlyings", dest="sp500_only", action="store_false", help="Disable S&P 500 filtering and keep every underlying from the source CSV.")
    parser.add_argument("--skip-existing", dest="skip_existing", action="store_true", default=DEFAULT_SKIP_EXISTING, help="Skip day-level output files that already have a matching .ok marker.")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false", help="Force re-download even if the day-level output file already exists.")
    parser.add_argument("--out-dir", default=OUT_DIR, help="Local output directory.")
    return parser


def resolve_date_range(args: argparse.Namespace) -> list[date]:
    if not args.start_date or not args.end_date:
        raise ValueError("--start-date and --end-date must be provided together.")
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    if start_date > end_date:
        raise ValueError("start date must be earlier than or equal to end date.")

    total_days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=offset) for offset in range(total_days)]


def read_underlyings(csv_path: str) -> list[str]:
    df = pd.read_csv(csv_path)
    candidate_columns = ["underlying_ticker", "symbol", "ticker"]
    source_column = next((column for column in candidate_columns if column in df.columns), None)
    if source_column is None:
        raise RuntimeError(f"CSV must contain one of {candidate_columns}: {csv_path}")
    values = df[source_column].dropna().astype(str).str.strip().str.replace(".", "-", regex=False)
    return sorted(v for v in values.unique().tolist() if v)


def load_sp500_symbols(csv_path: str) -> set[str]:
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"S&P 500 csv not found: {csv_path}")

    sp500_df = pd.read_csv(csv_path)
    candidate_columns = ["symbol", "ticker", "underlying_ticker"]
    source_column = next((column for column in candidate_columns if column in sp500_df.columns), None)
    if source_column is None:
        raise ValueError(
            f"S&P 500 csv must contain one of {candidate_columns}, got columns={sp500_df.columns.tolist()}"
        )

    symbols = (
        sp500_df[source_column]
        .dropna()
        .astype(str)
        .str.strip()
        .str.replace(".", "-", regex=False)
    )
    symbols = symbols[symbols != ""]
    return set(symbols.tolist())


def filter_underlyings(underlyings: list[str], allowed_symbols: set[str]) -> list[str]:
    return [ticker for ticker in underlyings if ticker in allowed_symbols]


def resolve_dataset(feed: str) -> str:
    mapping = {
        "trades": "trades_v1",
        "quotes": "quotes_v1",
    }
    return mapping[feed]


def build_prefix(year: str, month: str, day: str, dataset: str) -> str:
    return f"us_stocks_sip/{dataset}/{year}/{month}/{year}-{month}-{day}"


def create_s3_client():
    if not ACCESS_KEY or not SECRET_KEY:
        raise RuntimeError("Missing credentials. Set MASSIVE_AWS_KEY and MASSIVE_AWS_SECRET.")

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(
            signature_version="s3v4",
            connect_timeout=60,
            read_timeout=300,
            max_pool_connections=32,
            retries={"max_attempts": 10, "mode": "adaptive"},
        ),
    )


def list_daily_keys(s3, bucket: str, prefix: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv") or key.endswith(".csv.gz"):
                keys.append(key)
    return sorted(keys)


def marker_path(out_path: str) -> str:
    return f"{out_path}.ok"


def finalize_output(part_path: str, final_path: str) -> None:
    os.replace(part_path, final_path)
    with open(marker_path(final_path), "w", encoding="utf-8") as handle:
        handle.write("ok\n")


def remove_if_exists(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)


def build_cache_path(cache_dir: str, feed: str, year: str, month: str, filename: str) -> str:
    return os.path.join(cache_dir, feed, year, month, filename)


def segment_manifest_path(cache_path: str) -> str:
    return f"{cache_path}.segments.json"


def write_json_atomic(path: str, payload: dict) -> None:
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    os.replace(temp_path, path)


def build_segment_ranges(remote_size: int, segment_size: int) -> list[tuple[int, int]]:
    if remote_size <= 0:
        return []
    total_segments = math.ceil(remote_size / segment_size)
    ranges: list[tuple[int, int]] = []
    for index in range(total_segments):
        start = index * segment_size
        end = min(remote_size, start + segment_size) - 1
        ranges.append((start, end))
    return ranges


def segment_size_for_index(remote_size: int, segment_size: int, index: int) -> int:
    start = index * segment_size
    end = min(remote_size, start + segment_size)
    return max(0, end - start)


def completed_bytes_from_manifest(manifest: dict) -> int:
    remote_size = int(manifest["remote_size"])
    segment_size = int(manifest["segment_size"])
    completed_segments = manifest["completed_segments"]
    total = 0
    for index, done in enumerate(completed_segments):
        if done:
            total += segment_size_for_index(remote_size, segment_size, index)
    return total


def ensure_cache_file_size(cache_path: str, remote_size: int) -> None:
    with open(cache_path, "a+b") as handle:
        handle.truncate(remote_size)


def build_initial_completed_segments(remote_size: int, segment_size: int, contiguous_bytes: int) -> list[bool]:
    completed_segments = [False] * math.ceil(remote_size / segment_size)
    full_segments = contiguous_bytes // segment_size
    for index in range(min(full_segments, len(completed_segments))):
        completed_segments[index] = True
    return completed_segments


def load_or_create_segment_manifest(cache_path: str, remote_size: int, segment_size: int) -> dict:
    manifest_path = segment_manifest_path(cache_path)
    local_size = os.path.getsize(cache_path) if os.path.exists(cache_path) else 0

    if os.path.exists(manifest_path):
        with open(manifest_path, "r", encoding="utf-8") as handle:
            manifest = json.load(handle)
        expected_segments = math.ceil(remote_size / segment_size)
        if (
            int(manifest.get("remote_size", -1)) != remote_size
            or int(manifest.get("segment_size", -1)) != segment_size
            or len(manifest.get("completed_segments", [])) != expected_segments
        ):
            remove_if_exists(manifest_path)
        else:
            ensure_cache_file_size(cache_path, remote_size)
            return manifest

    if local_size > remote_size:
        remove_if_exists(cache_path)
        local_size = 0

    contiguous_bytes = min(local_size, remote_size)
    manifest = {
        "remote_size": remote_size,
        "segment_size": segment_size,
        "completed_segments": build_initial_completed_segments(remote_size, segment_size, contiguous_bytes),
    }
    ensure_cache_file_size(cache_path, remote_size)
    write_json_atomic(manifest_path, manifest)
    return manifest


def download_segment_to_cache(
    s3,
    bucket: str,
    key: str,
    cache_path: str,
    start: int,
    end: int,
    progress: tqdm,
) -> None:
    response = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
    bytes_remaining = end - start + 1
    offset = start

    with open(cache_path, "r+b") as handle:
        while bytes_remaining > 0:
            chunk = response["Body"].read(min(DEFAULT_DOWNLOAD_READ_CHUNK_BYTES, bytes_remaining))
            if not chunk:
                break
            handle.seek(offset)
            handle.write(chunk)
            chunk_size = len(chunk)
            offset += chunk_size
            bytes_remaining -= chunk_size
            progress.update(chunk_size)

    if bytes_remaining != 0:
        raise RuntimeError(
            f"incomplete segment download for s3://{bucket}/{key}: expected {end - start + 1} bytes, got {offset - start}"
        )


def remove_segment_manifest(cache_path: str) -> None:
    remove_if_exists(segment_manifest_path(cache_path))


def cleanup_residual_outputs(root_dirs: list[str]) -> tuple[int, int]:
    removed_parts = 0
    removed_orphan_markers = 0
    seen_roots: set[str] = set()

    for root_dir in root_dirs:
        if not root_dir:
            continue
        normalized_root = os.path.abspath(root_dir)
        if normalized_root in seen_roots or not os.path.exists(normalized_root):
            continue
        seen_roots.add(normalized_root)

        for current_root, _, files in os.walk(normalized_root):
            for filename in files:
                file_path = os.path.join(current_root, filename)
                if filename.endswith(".part"):
                    remove_if_exists(file_path)
                    removed_parts += 1
                    continue
                if filename.endswith(".ok"):
                    final_path = file_path[:-3]
                    if not os.path.exists(final_path):
                        remove_if_exists(file_path)
                        removed_orphan_markers += 1

    return removed_parts, removed_orphan_markers


def get_remote_object_size(s3, bucket: str, key: str) -> int:
    try:
        return int(s3.head_object(Bucket=bucket, Key=key)["ContentLength"])
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", ""))
        if error_code not in {"403", "405", "Forbidden", "MethodNotAllowed"}:
            raise

        response = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-0")
        content_range = response.get("ContentRange", "")
        if "/" in content_range:
            return int(content_range.rsplit("/", 1)[1])
        return int(response["ContentLength"])


def download_flatfile_to_cache(
    s3,
    bucket: str,
    key: str,
    cache_path: str,
    max_attempts: int,
) -> str:
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    remote_size = get_remote_object_size(s3, bucket, key)

    if os.path.exists(cache_path) and os.path.getsize(cache_path) == remote_size and not os.path.exists(segment_manifest_path(cache_path)):
        return cache_path

    manifest = load_or_create_segment_manifest(cache_path, remote_size, DEFAULT_RANGE_SEGMENT_BYTES)
    segment_ranges = build_segment_ranges(remote_size, DEFAULT_RANGE_SEGMENT_BYTES)
    total_segments = len(segment_ranges)

    last_error = None
    for attempt in range(1, max_attempts + 1):
        completed_segments = manifest["completed_segments"]
        pending_indices = [index for index, done in enumerate(completed_segments) if not done]
        if not pending_indices:
            remove_segment_manifest(cache_path)
            return cache_path

        try:
            completed_bytes = completed_bytes_from_manifest(manifest)
            if completed_bytes:
                print(
                    f"resuming cache {os.path.basename(cache_path)} with {len(pending_indices)} pending segment(s) / {total_segments} total "
                    f"({completed_bytes:,} / {remote_size:,} bytes complete)"
                )
            else:
                print(
                    f"downloading cache {os.path.basename(cache_path)} with {total_segments} segment(s) "
                    f"({remote_size:,} bytes total)"
                )

            with tqdm(
                total=remote_size,
                initial=completed_bytes,
                desc=f"download {os.path.basename(cache_path)}",
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                mininterval=0.2,
                leave=False,
            ) as progress:
                with ThreadPoolExecutor(max_workers=min(DEFAULT_S3_TRANSFER_CONCURRENCY, max(1, len(pending_indices)))) as executor:
                    future_to_index = {
                        executor.submit(
                            download_segment_to_cache,
                            s3,
                            bucket,
                            key,
                            cache_path,
                            segment_ranges[index][0],
                            segment_ranges[index][1],
                            progress,
                        ): index
                        for index in pending_indices
                    }
                    segment_errors: list[tuple[int, Exception]] = []
                    for future in as_completed(future_to_index):
                        index = future_to_index[future]
                        try:
                            future.result()
                        except Exception as exc:
                            segment_start, segment_end = segment_ranges[index]
                            print(
                                f"segment {index} failed for {os.path.basename(cache_path)} "
                                f"bytes={segment_start}-{segment_end}: {type(exc).__name__}: {exc}"
                            )
                            segment_errors.append((index, exc))
                            continue

                        manifest["completed_segments"][index] = True
                        write_json_atomic(segment_manifest_path(cache_path), manifest)

                    if segment_errors:
                        failed_indexes = ", ".join(str(index) for index, _ in segment_errors[:8])
                        if len(segment_errors) > 8:
                            failed_indexes = f"{failed_indexes}, ..."
                        print(
                            f"segment retry needed for {os.path.basename(cache_path)}: "
                            f"{len(segment_errors)} failed segment(s), indexes [{failed_indexes}]"
                        )
                        raise RuntimeError(
                            f"segment download failed for indexes [{failed_indexes}]"
                        ) from segment_errors[0][1]

            final_size = os.path.getsize(cache_path) if os.path.exists(cache_path) else 0
            if final_size != remote_size or not all(manifest["completed_segments"]):
                raise RuntimeError(
                    f"incomplete cache download for s3://{bucket}/{key}: expected {remote_size}, got {final_size}, completed segments={sum(manifest['completed_segments'])}/{total_segments}"
                )
            remove_segment_manifest(cache_path)
            return cache_path
        except Exception as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            time.sleep(DEFAULT_RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError(f"failed to download cache for s3://{bucket}/{key} after {max_attempts} attempts: {last_error}") from last_error


def stream_filter_stocks_via_s3(
    s3,
    bucket: str,
    key: str,
    out_path: str,
    cache_path: str,
    allowed_tickers: set[str],
    chunksize: int,
    download_retries: int,
) -> int:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    part_path = f"{out_path}.part"
    marker_fp = marker_path(out_path)
    remove_if_exists(part_path)
    remove_if_exists(marker_fp)

    cached_input_path = download_flatfile_to_cache(s3, bucket, key, cache_path, max_attempts=max(1, download_retries))

    total_rows = 0
    wrote_header = False
    columns = None
    compression = "gzip" if cached_input_path.endswith(".gz") else None

    reader = pd.read_csv(cached_input_path, compression=compression, chunksize=chunksize)

    for chunk in reader:
        columns = chunk.columns.tolist()
        ticker_series = chunk["ticker"].astype("string").str.replace(".", "-", regex=False)
        filtered_chunk = chunk.loc[ticker_series.isin(allowed_tickers)].copy()
        if filtered_chunk.empty:
            continue

        filtered_chunk.to_csv(
            part_path,
            index=False,
            mode="a" if wrote_header else "w",
            header=not wrote_header,
            compression="gzip",
        )
        total_rows += len(filtered_chunk)
        wrote_header = True

    if not wrote_header:
        pd.DataFrame(columns=columns if columns is not None else ["ticker"]).to_csv(
            part_path,
            index=False,
            compression="gzip",
        )

    finalize_output(part_path, out_path)
    print(f"filtered {total_rows:,} rows: s3://{bucket}/{key} -> {out_path} (cache: {cached_input_path})")
    return total_rows


def download_stock_file(
    s3,
    feed: str,
    year: str,
    month: str,
    day: str,
    out_dir: str,
    cache_dir: str,
    allowed_tickers: set[str],
    chunksize: int,
    skip_existing: bool,
    download_retries: int,
) -> str | None:
    dataset = resolve_dataset(feed)
    prefix = build_prefix(year, month, day, dataset)

    try:
        keys = list_daily_keys(s3, BUCKET_NAME, prefix)
    except ClientError as exc:
        raise RuntimeError(f"failed to list {prefix}: {exc}") from exc

    if not keys:
        print(f"skipping {year}-{month}-{day}: no flatfiles found under prefix {prefix}")
        return None

    if len(keys) != 1:
        raise RuntimeError(f"expected exactly one stock flatfile for {year}-{month}-{day}, got {len(keys)}")

    out_path = os.path.join(out_dir, feed, year, month, f"{year}-{month}-{day}.csv.gz")
    cache_path = build_cache_path(cache_dir, feed, year, month, f"{year}-{month}-{day}.csv.gz")
    if skip_existing and os.path.exists(out_path) and os.path.exists(marker_path(out_path)):
        return out_path

    total_rows = stream_filter_stocks_via_s3(
        s3,
        BUCKET_NAME,
        keys[0],
        out_path,
        cache_path,
        allowed_tickers,
        chunksize,
        download_retries,
    )
    print(f"[{year}-{month}-{day}] {feed}: wrote {total_rows:,} rows for {len(allowed_tickers):,} tickers -> {out_path}")
    return out_path


def download_underlyings_for_date_range(
    *,
    underlyings: list[str],
    dates: list[date],
    feed: str,
    out_dir: str,
    cache_dir: str,
    chunksize: int,
    max_workers: int,
    skip_existing: bool,
    download_retries: int,
) -> list[str]:
    completed_paths: list[str] = []
    allowed_tickers = set(underlyings)

    if max_workers != 1:
        print(f"underlying download is running in serial day mode; ignoring --download-workers={max_workers}")

    s3_client = create_s3_client()
    with tqdm(total=len(dates), desc="Downloading underlyings", unit="day") as progress:
        for current_date in dates:
            year = f"{current_date.year:04d}"
            month = f"{current_date.month:02d}"
            day = f"{current_date.day:02d}"
            try:
                out_path = download_stock_file(
                    s3=s3_client,
                    feed=feed,
                    year=year,
                    month=month,
                    day=day,
                    out_dir=out_dir,
                    cache_dir=cache_dir,
                    allowed_tickers=allowed_tickers,
                    chunksize=chunksize,
                    skip_existing=skip_existing,
                    download_retries=download_retries,
                )
                if out_path:
                    completed_paths.append(out_path)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                raise RuntimeError(f"download failed for {current_date.isoformat()}: {exc}") from exc
            finally:
                progress.update(1)

    return completed_paths


def main():
    args = build_parser().parse_args()
    cleaned_parts, cleaned_markers = cleanup_residual_outputs([args.out_dir])
    if cleaned_parts or cleaned_markers:
        print(f"startup cleanup: removed {cleaned_parts} stale .part file(s) and {cleaned_markers} orphan .ok marker(s)")
    else:
        print("startup cleanup: no stale partial outputs found")

    dates = resolve_date_range(args)
    underlyings_csv = args.underlyings_csv
    print("reading underlyings:", underlyings_csv)
    underlyings = read_underlyings(underlyings_csv)

    if args.sp500_only:
        allowed_symbols = load_sp500_symbols(args.sp500_csv_path)
        before_filter = len(underlyings)
        underlyings = filter_underlyings(underlyings, allowed_symbols)
        print(
            f"filtered underlyings by S&P 500 universe: {len(underlyings)} kept / {before_filter} total "
            f"from {args.sp500_csv_path}"
        )

    if args.max_tickers:
        underlyings = underlyings[: args.max_tickers]

    print(f"loaded {len(underlyings)} underlyings for {args.start_date} to {args.end_date}")
    download_underlyings_for_date_range(
        underlyings=underlyings,
        dates=dates,
        feed=args.feed,
        out_dir=args.out_dir,
        cache_dir=args.download_cache_dir,
        chunksize=max(1, args.chunk_size),
        max_workers=max(1, args.download_workers),
        skip_existing=args.skip_existing,
        download_retries=max(1, args.download_retries),
    )


if __name__ == "__main__":
    main()
