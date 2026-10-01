import argparse
import io
import json
import math
import time
import os
import re
import gzip
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import boto3
import pandas as pd
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from tqdm import tqdm

# ====== 默认配置 ======
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_STOCK_ROOT = PROJECT_ROOT / "data" / "raw_data" / "stock"
DEFAULT_START_DATE = "2026-05-01"  # 默认起始日期，ISO 格式
DEFAULT_END_DATE = "2026-05-14"  # 默认结束日期，ISO 格式
DEFAULT_FEED = "trades"  # 可选: trades / quotes
DEFAULT_MODE = "both"  # 可选: download / extract-underlyings / both
DEFAULT_SP500_ONLY = True
DEFAULT_SP500_CSV_PATH = str(Path(__file__).with_name("sp500_historical_summary.csv"))
DEFAULT_CHUNK_SIZE = 1_000_000
DEFAULT_DOWNLOAD_WORKERS = 1
DEFAULT_SKIP_EXISTING = True
DEFAULT_DOWNLOAD_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 3.0
DEFAULT_S3_TRANSFER_CONCURRENCY = 16
DEFAULT_DOWNLOAD_CACHE_DIR = str(RAW_STOCK_ROOT / "cache" / "options")
DEFAULT_DOWNLOAD_READ_CHUNK_BYTES = 16 * 1024 * 1024
DEFAULT_RANGE_SEGMENT_BYTES = 512 * 1024 * 1024
DEFAULT_FILTER_GZIP_COMPRESSLEVEL = 1

ENDPOINT_URL = "https://files.massive.com"  # Massive flatfiles 的 S3-compatible endpoint
BUCKET_NAME = "flatfiles"  # Massive flatfiles bucket 名称

ACCESS_KEY = os.environ.get("MASSIVE_AWS_KEY", "")
SECRET_KEY = os.environ.get("MASSIVE_AWS_SECRET", "")

DOWNLOAD_OUT_DIR = str(RAW_STOCK_ROOT / "options")
SP500_DOWNLOAD_OUT_DIR = str(RAW_STOCK_ROOT / "options_sp500")
UNDERLYING_OUT_DIR = str(RAW_STOCK_ROOT / "option_underlyings")
# =====================

OPTION_TICKER_RE = re.compile(r"^O:(?P<underlying>.+?)(?=\d{6}[CP]\d{8}$)")  # 从 OPRA 期权代码里提取 underlying ticker
OPTION_TICKER_PATTERN = r"^O:(.+?)(?=\d{6}[CP]\d{8}$)"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download Massive options flatfiles or extract unique underlying stock tickers over a date range.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Inclusive start date in ISO format, for example 2026-04-12.")
    parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="Inclusive end date in ISO format, for example 2026-05-12.")
    parser.add_argument(
        "--mode",
        default=DEFAULT_MODE,
        choices=["download", "extract-underlyings", "both"],
        help="download: fetch options flatfiles, extract-underlyings: parse local file, both: do download then extraction.",
    )
    parser.add_argument(
        "--feed",
        default=DEFAULT_FEED,
        choices=["trades", "quotes"],
        help="Options flatfiles feed to download/read.",
    )
    parser.add_argument("--download-out-dir", default=DOWNLOAD_OUT_DIR, help="Root directory for downloaded options flatfiles.")
    parser.add_argument("--sp500-download-out-dir", default=SP500_DOWNLOAD_OUT_DIR, help="Root directory for S&P 500 filtered options flatfiles.")
    parser.add_argument("--underlying-out-dir", default=UNDERLYING_OUT_DIR, help="Output directory for extracted underlying ticker lists.")
    parser.add_argument("--download-cache-dir", default=DEFAULT_DOWNLOAD_CACHE_DIR, help="Root directory for resumable raw flatfile cache files.")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Rows per streamed CSV chunk when reading flatfiles.")
    parser.add_argument("--download-workers", type=int, default=DEFAULT_DOWNLOAD_WORKERS, help="Reserved for compatibility. Option quote flatfiles are downloaded serially, one day at a time.")
    parser.add_argument("--download-retries", type=int, default=DEFAULT_DOWNLOAD_RETRIES, help="Maximum retry attempts for a single day option flatfile download.")
    parser.add_argument("--sp500-csv-path", default=DEFAULT_SP500_CSV_PATH, help="CSV path used to load S&P 500 symbols. The file must contain one of: symbol, ticker, underlying_ticker.")
    parser.add_argument("--sp500-only", dest="sp500_only", action="store_true", default=DEFAULT_SP500_ONLY, help="Only keep option rows whose underlying is in the S&P 500 symbol CSV.")
    parser.add_argument("--all-underlyings", dest="sp500_only", action="store_false", help="Disable S&P 500 filtering and keep the full daily flatfile.")
    parser.add_argument("--skip-existing", dest="skip_existing", action="store_true", default=DEFAULT_SKIP_EXISTING, help="Skip downloaded files that already have a matching .ok marker.")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false", help="Force re-download even if the target file already exists.")
    return parser


def resolve_date_range(args: argparse.Namespace) -> list[date]:
    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise ValueError("--start-date and --end-date must be provided together.")
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
    else:
        start_date = date(int(args.year), int(args.month), int(args.day))
        end_date = start_date

    if start_date > end_date:
        raise ValueError("start date must be earlier than or equal to end date.")

    total_days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=offset) for offset in range(total_days)]


def resolve_dataset(feed: str) -> str:
    mapping = {
        "trades": "trades_v1",
        "quotes": "quotes_v1",
    }
    return mapping[feed]


def build_prefix(year: str, month: str, day: str, dataset: str) -> str:
    return f"us_options_opra/{dataset}/{year}/{month}/{year}-{month}-{day}"


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

    raise RuntimeError(f"failed to download s3://{bucket}/{key} after {max_attempts} attempts: {last_error}") from last_error


def load_sp500_symbols(csv_path: str) -> set[str]:
    if not os.path.exists(csv_path):
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


def extract_underlying_series(ticker_series: pd.Series) -> pd.Series:
    values = ticker_series.astype("string")
    cp_flag = values.str.slice(-9, -8)
    is_valid = values.str.startswith("O:") & values.str.len().ge(17) & cp_flag.isin(["C", "P"])
    underlying = pd.Series(pd.NA, index=values.index, dtype="string")
    underlying.loc[is_valid] = values.loc[is_valid].str.slice(2, -15)
    return underlying


class ProgressFileReader:
    def __init__(self, file_obj, progress: tqdm):
        self._file_obj = file_obj
        self._progress = progress

    def read(self, size: int = -1):
        before = self._file_obj.tell()
        data = self._file_obj.read(size)
        after = self._file_obj.tell()
        self._progress.update(max(0, after - before))
        return data

    def readline(self, size: int = -1):
        before = self._file_obj.tell()
        data = self._file_obj.readline(size)
        after = self._file_obj.tell()
        self._progress.update(max(0, after - before))
        return data

    def seek(self, offset: int, whence: int = 0):
        return self._file_obj.seek(offset, whence)

    def tell(self):
        return self._file_obj.tell()

    def seekable(self):
        return self._file_obj.seekable()

    def readable(self):
        return self._file_obj.readable()

    def close(self):
        return self._file_obj.close()

    @property
    def closed(self):
        return self._file_obj.closed

    def __getattr__(self, name):
        return getattr(self._file_obj, name)


def stream_copy_flatfile_via_s3(
    s3,
    bucket: str,
    key: str,
    out_path: str,
    cache_path: str,
    chunksize: int = DEFAULT_CHUNK_SIZE,
    download_retries: int = DEFAULT_DOWNLOAD_RETRIES,
) -> int:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    part_path = f"{out_path}.part"
    remove_if_exists(part_path)
    remove_if_exists(marker_path(out_path))

    cached_input_path = download_flatfile_to_cache(s3, bucket, key, cache_path, max_attempts=max(1, download_retries))
    bytes_written = os.path.getsize(cached_input_path)
    shutil.copyfile(cached_input_path, part_path)
    finalize_output(part_path, out_path)

    print(f"copied {bytes_written:,} bytes: s3://{bucket}/{key} -> {out_path}")
    return bytes_written


def stream_filter_sp500_options_via_s3(
    s3,
    bucket: str,
    key: str,
    out_path: str,
    cache_path: str,
    allowed_underlyings: set[str],
    chunksize: int = DEFAULT_CHUNK_SIZE,
    download_retries: int = DEFAULT_DOWNLOAD_RETRIES,
) -> int:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    part_path = f"{out_path}.part"
    remove_if_exists(part_path)
    remove_if_exists(marker_path(out_path))

    cached_input_path = download_flatfile_to_cache(s3, bucket, key, cache_path, max_attempts=max(1, download_retries))

    total_rows = 0
    wrote_header = False
    columns = None

    input_size = os.path.getsize(cached_input_path)
    with tqdm(
        total=input_size,
        desc=f"filter {os.path.basename(cached_input_path)}",
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        mininterval=0.2,
        leave=False,
    ) as progress, open(cached_input_path, "rb") as raw_input, gzip.open(
        part_path,
        "wt",
        newline="",
        encoding="utf-8",
        compresslevel=DEFAULT_FILTER_GZIP_COMPRESSLEVEL,
    ) as handle:
        tracked_input = ProgressFileReader(raw_input, progress)
        if cached_input_path.endswith(".gz"):
            gzip_input = gzip.GzipFile(fileobj=tracked_input, mode="rb")
            text_input = io.TextIOWrapper(gzip_input, encoding="utf-8", newline="")
        else:
            gzip_input = None
            text_input = io.TextIOWrapper(tracked_input, encoding="utf-8", newline="")

        try:
            reader = pd.read_csv(
                text_input,
                chunksize=chunksize,
                dtype={"ticker": "string"},
                low_memory=False,
                na_filter=False,
            )
            for chunk in reader:
                columns = chunk.columns.tolist()
                underlying = extract_underlying_series(chunk["ticker"])
                filtered_chunk = chunk.loc[underlying.isin(allowed_underlyings)]
                if filtered_chunk.empty:
                    continue

                filtered_chunk.to_csv(handle, index=False, header=not wrote_header)
                total_rows += len(filtered_chunk)
                wrote_header = True

            if not wrote_header:
                pd.DataFrame(columns=columns if columns is not None else ["ticker"]).to_csv(handle, index=False)
        finally:
            text_input.close()
            if gzip_input is not None:
                gzip_input.close()

    finalize_output(part_path, out_path)
    print(f"filtered {total_rows:,} rows: s3://{bucket}/{key} -> {out_path} (cache: {cached_input_path})")
    return total_rows


def download_options_file(
    s3,
    feed: str,
    year: str,
    month: str,
    day: str,
    out_dir: str,
    cache_dir: str,
    sp500_only: bool = False,
    allowed_underlyings: set[str] | None = None,
    chunksize: int = DEFAULT_CHUNK_SIZE,
    skip_existing: bool = True,
    download_retries: int = DEFAULT_DOWNLOAD_RETRIES,
) -> str | None:
    dataset = resolve_dataset(feed)
    prefix = build_prefix(year, month, day, dataset)
    print("listing prefix:", prefix)

    try:
        keys = list_daily_keys(s3, BUCKET_NAME, prefix)
    except ClientError as exc:
        raise RuntimeError(f"failed to list {prefix}: {exc}") from exc

    if not keys:
        print(f"skipping {year}-{month}-{day}: no flatfiles found under prefix {prefix}")
        return None

    if len(keys) != 1:
        print(f"found {len(keys)} file(s), downloading all")

    last_out_path = ""
    for key in keys:
        filename = os.path.basename(key)
        out_path = os.path.join(out_dir, feed, year, month, filename)
        cache_path = build_cache_path(cache_dir, feed, year, month, filename)
        if skip_existing and os.path.exists(out_path) and os.path.exists(marker_path(out_path)):
            last_out_path = out_path
            continue
        if sp500_only:
            if not allowed_underlyings:
                raise RuntimeError("sp500_only=True requires a non-empty allowed_underlyings set.")
            stream_filter_sp500_options_via_s3(
                s3,
                BUCKET_NAME,
                key,
                out_path,
                cache_path,
                allowed_underlyings,
                chunksize=chunksize,
                download_retries=download_retries,
            )
        else:
            stream_copy_flatfile_via_s3(
                s3,
                BUCKET_NAME,
                key,
                out_path,
                cache_path,
                chunksize=chunksize,
                download_retries=download_retries,
            )
        last_out_path = out_path
    return last_out_path


def build_input_path(root_dir: str, feed: str, year: str, month: str, day: str) -> str:
    candidates = [
        os.path.join(root_dir, feed, year, month, f"{year}-{month}-{day}.csv.gz"),
        os.path.join(root_dir, feed, year, month, f"{year}-{month}-{day}.csv"),
        os.path.join(root_dir, feed, f"{year}-{month}-{day}.csv.gz"),
        os.path.join(root_dir, feed, f"{year}-{month}-{day}.csv"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"No options file found for {feed} {year}-{month}-{day} under {root_dir}")


def extract_underlying(option_ticker: str) -> str | None:
    if not isinstance(option_ticker, str):
        return None
    match = OPTION_TICKER_RE.match(option_ticker)
    if not match:
        return None
    return match.group("underlying")


def load_unique_underlyings(input_path: str, chunksize: int = DEFAULT_CHUNK_SIZE) -> set[str]:
    unique_underlyings: set[str] = set()

    for chunk in pd.read_csv(input_path, usecols=["ticker"], chunksize=chunksize):
        underlyings = extract_underlying_series(chunk["ticker"]).dropna()
        if underlyings.empty:
            continue
        unique_underlyings.update(underlyings.astype(str).tolist())

    return unique_underlyings


def write_output(underlyings: pd.Series, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    part_path = f"{out_path}.part"
    remove_if_exists(part_path)
    remove_if_exists(marker_path(out_path))
    out_df = pd.DataFrame({"underlying_ticker": underlyings})
    out_df.to_csv(part_path, index=False)
    finalize_output(part_path, out_path)
    print(f"wrote {len(out_df)} unique underlyings -> {out_path}")


def build_underlyings_out_path(out_dir: str, feed: str, start_label: str, end_label: str) -> str:
    return os.path.join(out_dir, feed, f"{start_label}_{end_label}_underlyings.csv")


def extract_underlyings_for_date_range(feed: str, dates: list[date], input_dir: str, out_dir: str) -> str:
    all_underlyings: set[str] = set()
    start_label = dates[0].isoformat()
    end_label = dates[-1].isoformat()

    for current_date in tqdm(dates, desc="Extracting underlyings"):
        year = f"{current_date.year:04d}"
        month = f"{current_date.month:02d}"
        day = f"{current_date.day:02d}"
        try:
            input_path = build_input_path(input_dir, feed, year, month, day)
        except FileNotFoundError:
            print(f"skipping {year}-{month}-{day}: local options file not found")
            continue
        print("reading:", input_path)
        all_underlyings.update(load_unique_underlyings(input_path))

    if not all_underlyings:
        raise RuntimeError(f"no local options files found for feed={feed} in range {start_label} to {end_label}")

    underlyings = pd.Series(sorted(all_underlyings), dtype="string")
    out_path = build_underlyings_out_path(out_dir, feed, start_label, end_label)
    write_output(underlyings, out_path)
    print(underlyings.head(20).to_string(index=False))
    return out_path


def download_options_for_date_range(
    *,
    dates: list[date],
    feed: str,
    out_dir: str,
    cache_dir: str,
    sp500_only: bool,
    allowed_underlyings: set[str] | None,
    chunksize: int,
    max_workers: int,
    skip_existing: bool,
    download_retries: int,
) -> list[str]:
    completed_paths: list[str] = []

    if max_workers != 1:
        print(f"quotes download is running in serial day mode; ignoring --download-workers={max_workers}")

    s3_client = create_s3_client()
    with tqdm(total=len(dates), desc="Downloading option files", unit="day") as progress:
        for current_date in dates:
            year = f"{current_date.year:04d}"
            month = f"{current_date.month:02d}"
            day = f"{current_date.day:02d}"
            try:
                out_path = download_options_file(
                    s3=s3_client,
                    feed=feed,
                    year=year,
                    month=month,
                    day=day,
                    out_dir=out_dir,
                    cache_dir=cache_dir,
                    sp500_only=sp500_only,
                    allowed_underlyings=allowed_underlyings,
                    chunksize=chunksize,
                    skip_existing=skip_existing,
                    download_retries=download_retries,
                )
                if out_path:
                    completed_paths.append(out_path)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                iso_date = current_date.isoformat()
                raise RuntimeError(f"download failed for {iso_date}: {exc}") from exc
            finally:
                progress.update(1)

    return completed_paths


def main():
    args = build_parser().parse_args()
    cleaned_parts, cleaned_markers = cleanup_residual_outputs(
        [
            args.download_out_dir,
            args.sp500_download_out_dir,
            args.underlying_out_dir,
        ]
    )
    if cleaned_parts or cleaned_markers:
        print(f"startup cleanup: removed {cleaned_parts} stale .part file(s) and {cleaned_markers} orphan .ok marker(s)")
    else:
        print("startup cleanup: no stale partial outputs found")

    dates = resolve_date_range(args)
    if args.sp500_only and not args.sp500_csv_path:
        raise ValueError("--sp500-only requires --sp500-csv-path.")

    allowed_underlyings = load_sp500_symbols(args.sp500_csv_path) if args.sp500_only else None
    input_dir = args.sp500_download_out_dir if args.sp500_only else args.download_out_dir

    if args.sp500_only:
        print(f"loaded {len(allowed_underlyings):,} S&P 500 symbols from {args.sp500_csv_path}")
    else:
        print("S&P 500 filtering disabled; streaming full OPRA flatfiles.")

    if args.mode in {"download", "both"}:
        download_options_for_date_range(
            dates=dates,
            feed=args.feed,
            out_dir=input_dir,
            cache_dir=args.download_cache_dir,
            sp500_only=args.sp500_only,
            allowed_underlyings=allowed_underlyings,
            chunksize=max(1, args.chunk_size),
            max_workers=max(1, args.download_workers),
            skip_existing=args.skip_existing,
            download_retries=max(1, args.download_retries),
        )

    if args.mode in {"extract-underlyings", "both"}:
        extract_underlyings_for_date_range(args.feed, dates, input_dir, args.underlying_out_dir)


if __name__ == "__main__":
    main()
