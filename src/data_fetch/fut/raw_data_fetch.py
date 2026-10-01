import argparse
import json
import logging
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

try:
	from tqdm.auto import tqdm
except ModuleNotFoundError:
	tqdm = None


DEFAULT_API_KEY = os.environ.get("DATABENTO_API_KEY", "")
DEFAULT_DATASET = "GLBX.MDP3"
DEFAULT_FUTURES = ['GCM6', 'GCQ6', 'GCV6']
DEFAULT_START_DATE = "2026-05-10"
DEFAULT_END_DATE = "2026-05-27"
DEFAULT_SCHEMAS = "trades,mbp-10"
DEFAULT_TRADING_CLASSES = "OG"
DEFAULT_DEFINITION_CHUNK_ROWS = 250_000
DEFAULT_SYMBOL_BATCH_SIZE = 200
DEFAULT_WORKERS = 4
DEFAULT_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 3.0
DEFAULT_STRIKE_MIN = 2500
DEFAULT_STRIKE_MAX = 6500
DEFAULT_SKIP_EXISTING = True
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_ENABLE_PROGRESS = True
DEFAULT_MODE = "both"
LOG_LEVEL_CHOICES = ["DEBUG", "INFO", "WARNING", "ERROR"]
MODE_CHOICES = ["scan-definitions", "download-data", "both"]

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "raw_data" / "fut"
DEFAULT_DEFINITION_CACHE_DIR = DEFAULT_OUTPUT_ROOT / "definition_cache"
DEFAULT_DEFINITION_FILTER_DIR = DEFAULT_OUTPUT_ROOT / "definition_filtered"
DEFAULT_UNDERLYING_DATA_DIR = DEFAULT_OUTPUT_ROOT / "raw_db_und"
DEFAULT_OPTION_DATA_DIR = DEFAULT_OUTPUT_ROOT / "raw_db_opt"
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs" / "data_fetch" / "fut"

LOGGER = logging.getLogger("raw_data_fetch")


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(
		description="Fetch Databento futures and options-on-futures data by first scanning definition records, then downloading matched symbols."
	)
	parser.add_argument("--api-key", default=DEFAULT_API_KEY, help="Databento API key. Defaults to DATABENTO_API_KEY.")
	parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Databento dataset, for example GLBX.MDP3.")
	parser.add_argument(
		"--futures",
		default=DEFAULT_FUTURES,
		help="Comma-separated futures symbols used for underlying downloads, for example GCM6,GCQ6,GCV6.",
	)
	parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Inclusive start date in ISO format, for example 2026-05-20.")
	parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="Inclusive end date in ISO format, for example 2026-05-22.")
	parser.add_argument(
		"--schemas",
		default=DEFAULT_SCHEMAS,
		help="Comma-separated Databento schemas to download for both futures and options, for example trades,mbo,mbp10.",
	)
	parser.add_argument(
		"--trading-classes",
		default=DEFAULT_TRADING_CLASSES,
		help="Comma-separated definition.asset filters applied during option definition selection. Leave empty to disable this filter, for example G1M.",
	)
	parser.add_argument("--strike-min", type=float, default=DEFAULT_STRIKE_MIN, help="Minimum strike price to keep during definition filtering.")
	parser.add_argument("--strike-max", type=float, default=DEFAULT_STRIKE_MAX, help="Maximum strike price to keep during definition filtering.")
	parser.add_argument("--definition-chunk-rows", type=int, default=DEFAULT_DEFINITION_CHUNK_ROWS, help="Rows per DataFrame chunk when filtering definition DBN files.")
	parser.add_argument("--symbol-batch-size", type=int, default=DEFAULT_SYMBOL_BATCH_SIZE, help="Maximum option symbols per Databento data request.")
	parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Maximum concurrent download tasks across date, schema, and symbol batches.")
	parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Maximum retry attempts per definition or data task.")
	parser.add_argument("--retry-backoff-seconds", type=float, default=DEFAULT_RETRY_BACKOFF_SECONDS, help="Linear backoff in seconds between retries.")
	parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Root directory for outputs, cache files, and manifests.")
	parser.add_argument("--definition-cache-dir", default=str(DEFAULT_DEFINITION_CACHE_DIR), help="Directory for streamed raw definition DBN files.")
	parser.add_argument("--definition-filter-dir", default=str(DEFAULT_DEFINITION_FILTER_DIR), help="Directory for filtered definition parquet/json outputs.")
	parser.add_argument("--underlying-data-dir", default=str(DEFAULT_UNDERLYING_DATA_DIR), help="Directory for streamed futures data DBN files.")
	parser.add_argument("--option-data-dir", default=str(DEFAULT_OPTION_DATA_DIR), help="Directory for streamed options data DBN files.")
	parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR), help="Directory for run logs.")
	parser.add_argument("--log-level", default=DEFAULT_LOG_LEVEL, choices=LOG_LEVEL_CHOICES, help="Logging level for console and file output.")
	parser.add_argument("--progress", dest="progress", action="store_true", default=DEFAULT_ENABLE_PROGRESS, help="Show progress bars when tqdm is available.")
	parser.add_argument("--no-progress", dest="progress", action="store_false", help="Disable progress bars.")
	parser.add_argument("--skip-existing", dest="skip_existing", action="store_true", default=DEFAULT_SKIP_EXISTING, help="Skip tasks with completed .ok markers.")
	parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false", help="Force re-run even when completed outputs already exist.")
	parser.add_argument(
		"--mode",
		default=DEFAULT_MODE,
		choices=MODE_CHOICES,
		help="scan-definitions: only build filtered option symbol manifests, download-data: use existing manifests to fetch data, both: scan then download.",
	)
	return parser


class NullProgress:
	def __init__(self, total: int | None = None, desc: str | None = None):
		self.total = total
		self.desc = desc

	def update(self, step: int = 1) -> None:
		return None

	def set_postfix_str(self, _: str) -> None:
		return None

	def close(self) -> None:
		return None


def create_progress(total: int, desc: str, enabled: bool):
	if enabled and tqdm is not None:
		return tqdm(total=total, desc=desc, dynamic_ncols=True)
	return NullProgress(total=total, desc=desc)


def setup_logging(log_dir: Path, level: str) -> Path:
	log_dir.mkdir(parents=True, exist_ok=True)
	log_path = log_dir / "raw_data_fetch.log"
	logger = logging.getLogger("raw_data_fetch")
	logger.handlers.clear()
	logger.setLevel(getattr(logging, level.upper(), logging.INFO))
	logger.propagate = False

	formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

	file_handler = logging.FileHandler(log_path, encoding="utf-8")
	file_handler.setFormatter(formatter)
	logger.addHandler(file_handler)

	stream_handler = logging.StreamHandler()
	stream_handler.setFormatter(formatter)
	logger.addHandler(stream_handler)

	return log_path


def parse_iso_timestamp(value: str) -> datetime:
	return datetime.fromisoformat(value.replace("Z", "+00:00"))


def utc_day_start(day: date) -> datetime:
	return datetime.combine(day, dt_time.min, tzinfo=timezone.utc)


def utc_day_end_exclusive(day: date) -> datetime:
	return utc_day_start(day + timedelta(days=1))


def marker_path(path: Path) -> Path:
	return Path(f"{path}.ok")


def finalize_output(part_path: Path, final_path: Path) -> None:
	final_path.parent.mkdir(parents=True, exist_ok=True)
	os.replace(part_path, final_path)
	marker_path(final_path).write_text("ok\n", encoding="utf-8")


def remove_if_exists(path: Path) -> None:
	if path.exists():
		path.unlink()


def resolve_date_range(start_date: str, end_date: str) -> list[date]:
	start = date.fromisoformat(start_date)
	end = date.fromisoformat(end_date)
	if start > end:
		raise ValueError("start date must be earlier than or equal to end date")
	total_days = (end - start).days + 1
	return [start + timedelta(days=offset) for offset in range(total_days)]


def parse_csv_arg(value: str | list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
	if value is None:
		return []
	if isinstance(value, str):
		return [item.strip() for item in value.split(",") if item.strip()]
	if isinstance(value, (list, tuple, set)):
		return [str(item).strip() for item in value if str(item).strip()]
	text = str(value).strip()
	return [text] if text else []


def ensure_api_key(api_key: str) -> str:
	if not api_key:
		raise RuntimeError("Missing Databento API key. Pass --api-key or set DATABENTO_API_KEY.")
	return api_key


def load_databento():
	try:
		import databento as db
	except ModuleNotFoundError as exc:
		raise RuntimeError("databento is not installed. Run: pip install databento") from exc
	return db


def create_client(api_key: str):
	db = load_databento()
	return db.Historical(api_key)


def clamp_query_window(client, dataset: str, schema: str, start_dt: datetime, end_dt: datetime) -> tuple[datetime, datetime] | None:
	dataset_range = client.metadata.get_dataset_range(dataset=dataset)
	schema_range = dataset_range.get("schema", {}).get(schema, dataset_range)
	available_start = parse_iso_timestamp(schema_range["start"])
	available_end = parse_iso_timestamp(schema_range["end"])
	query_start = max(start_dt, available_start)
	query_end = min(end_dt, available_end)
	if query_end <= query_start:
		return None
	return query_start, query_end


def chunked(values: list[str], chunk_size: int) -> Iterable[list[str]]:
	for index in range(0, len(values), chunk_size):
		yield values[index : index + chunk_size]


def normalize_instrument_class(series: pd.Series) -> pd.Series:
	return series.astype(str).str.upper()


def extract_symbol_column(df: pd.DataFrame) -> str:
	for column in ("raw_symbol", "symbol"):
		if column in df.columns:
			return column
	raise KeyError(f"Could not find symbol column in definition output: {df.columns.tolist()}")


def iter_store_frames(store, chunk_rows: int) -> Iterable[pd.DataFrame]:
	frames = store.to_df(count=chunk_rows)
	if isinstance(frames, pd.DataFrame):
		yield frames
		return
	for frame in frames:
		if frame is not None and not frame.empty:
			yield frame


def strike_mask(df: pd.DataFrame, strike_min: float | None, strike_max: float | None) -> pd.Series:
	if "strike_price" not in df.columns:
		return pd.Series(True, index=df.index)
	values = pd.to_numeric(df["strike_price"], errors="coerce")
	mask = values.notna()
	if strike_min is not None:
		mask &= values >= strike_min
	if strike_max is not None:
		mask &= values <= strike_max
	return mask


def trading_class_mask(df: pd.DataFrame, trading_classes: list[str]) -> pd.Series:
	if not trading_classes:
		return pd.Series(True, index=df.index)

	normalized_classes = {value.strip().upper() for value in trading_classes if value.strip()}
	if not normalized_classes:
		return pd.Series(True, index=df.index)
	if "asset" not in df.columns:
		return pd.Series(False, index=df.index)
	values = df["asset"].fillna("").astype(str).str.upper().str.strip()
	return values.isin(normalized_classes)


@dataclass(frozen=True)
class DefinitionSelection:
	day: str
	query_start: str
	query_end: str
	future_symbols: list[str]
	option_symbols: list[str]
	option_symbols_by_future: dict[str, list[str]]
	definition_rows: int
	output_parquet: str
	futures_output_parquet: str | None = None


def definition_cache_path(definition_cache_dir: Path, day: date) -> Path:
	return definition_cache_dir / f"{day.isoformat()}.definition.dbn.zst"


def definition_filtered_paths(definition_filter_dir: Path, day: date) -> tuple[Path, Path, Path, Path]:
	year = f"{day.year:04d}"
	month = f"{day.month:02d}"
	opt_root = definition_filter_dir / "opt" / year / month
	fut_root = definition_filter_dir / "fut" / year / month
	opt_parquet_path = opt_root / f"{day.isoformat()}.options.parquet"
	opt_manifest_path = opt_root / f"{day.isoformat()}.options.json"
	fut_parquet_path = fut_root / f"{day.isoformat()}.futures.parquet"
	fut_manifest_path = fut_root / f"{day.isoformat()}.futures.json"
	return opt_parquet_path, opt_manifest_path, fut_parquet_path, fut_manifest_path


def download_definition_snapshot(
	client,
	dataset: str,
	day: date,
	out_path: Path,
	retries: int,
	retry_backoff_seconds: float,
	skip_existing: bool,
) -> Path | None:
	out_path.parent.mkdir(parents=True, exist_ok=True)
	if skip_existing and out_path.exists() and marker_path(out_path).exists():
		LOGGER.info("skip existing definition cache: %s", out_path)
		return out_path

	window = clamp_query_window(client, dataset, "definition", utc_day_start(day), utc_day_end_exclusive(day))
	if window is None:
		LOGGER.warning("no definition window available for %s", day.isoformat())
		return None
	query_start, query_end = window

	part_path = Path(f"{out_path}.part")
	remove_if_exists(part_path)
	remove_if_exists(marker_path(out_path))

	last_error = None
	for attempt in range(1, retries + 1):
		try:
			client.timeseries.get_range(
				dataset=dataset,
				schema="definition",
				symbols="ALL_SYMBOLS",
				start=query_start.isoformat(),
				end=query_end.isoformat(),
				path=str(part_path),
			)
			finalize_output(part_path, out_path)
			LOGGER.info("downloaded definition snapshot %s -> %s", day.isoformat(), out_path)
			return out_path
		except Exception as exc:
			last_error = exc
			remove_if_exists(part_path)
			if attempt == retries:
				break
			time.sleep(retry_backoff_seconds * attempt)

	raise RuntimeError(f"failed to download definition snapshot for {day.isoformat()}: {last_error}") from last_error


def filter_definition_snapshot(
	db,
	day: date,
	dbn_path: Path,
	future_symbols: list[str],
	trading_classes: list[str],
	strike_min: float | None,
	strike_max: float | None,
	chunk_rows: int,
	opt_parquet_path: Path,
	opt_manifest_path: Path,
	fut_parquet_path: Path,
	fut_manifest_path: Path,
	skip_existing: bool,
) -> DefinitionSelection:
	opt_parquet_path.parent.mkdir(parents=True, exist_ok=True)
	fut_parquet_path.parent.mkdir(parents=True, exist_ok=True)
	if (
		skip_existing
		and opt_parquet_path.exists()
		and marker_path(opt_parquet_path).exists()
		and opt_manifest_path.exists()
		and fut_parquet_path.exists()
		and marker_path(fut_parquet_path).exists()
		and fut_manifest_path.exists()
	):
		manifest = json.loads(opt_manifest_path.read_text(encoding="utf-8"))
		return DefinitionSelection(**manifest)

	opt_part_path = Path(f"{opt_parquet_path}.part")
	opt_manifest_part_path = Path(f"{opt_manifest_path}.part")
	fut_part_path = Path(f"{fut_parquet_path}.part")
	fut_manifest_part_path = Path(f"{fut_manifest_path}.part")
	remove_if_exists(opt_part_path)
	remove_if_exists(marker_path(opt_parquet_path))
	remove_if_exists(opt_manifest_part_path)
	remove_if_exists(fut_part_path)
	remove_if_exists(marker_path(fut_parquet_path))
	remove_if_exists(fut_manifest_part_path)

	store = db.DBNStore.from_file(str(dbn_path))
	option_frames: list[pd.DataFrame] = []
	future_frames: list[pd.DataFrame] = []
	total_rows = 0

	for frame in iter_store_frames(store, chunk_rows):
		total_rows += len(frame)
		if "instrument_class" not in frame.columns:
			continue
		instrument_class = normalize_instrument_class(frame["instrument_class"])

		if "underlying" in frame.columns:
			underlying_mask = frame["underlying"].astype(str).isin(future_symbols)
			option_mask = (
				instrument_class.isin(["C", "P"])
				& underlying_mask
				& trading_class_mask(frame, trading_classes)
				& strike_mask(frame, strike_min, strike_max)
			)
			filtered_options = frame.loc[option_mask].copy()
			if not filtered_options.empty:
				option_frames.append(filtered_options)

		try:
			symbol_column = extract_symbol_column(frame)
		except KeyError:
			continue

		future_mask = instrument_class.eq("F") & frame[symbol_column].astype(str).isin(future_symbols)
		filtered_futures = frame.loc[future_mask].copy()
		if not filtered_futures.empty:
			future_frames.append(filtered_futures)

	if option_frames:
		selected = pd.concat(option_frames, ignore_index=True)
	else:
		selected = pd.DataFrame(columns=["raw_symbol", "underlying", "instrument_class", "strike_price", "expiration"])

	if future_frames:
		selected_futures = pd.concat(future_frames, ignore_index=True)
	else:
		selected_futures = pd.DataFrame(columns=["raw_symbol", "instrument_class", "expiration"])

	if not selected.empty and "raw_symbol" not in selected.columns:
		raise KeyError(f"definition output is missing required raw_symbol column: {selected.columns.tolist()}")
	if not selected_futures.empty and "raw_symbol" not in selected_futures.columns:
		raise KeyError(f"futures definition output is missing required raw_symbol column: {selected_futures.columns.tolist()}")
	symbol_column = "raw_symbol"
	option_symbols = []
	option_symbols_by_future = {future_symbol: [] for future_symbol in future_symbols}
	if symbol_column in selected.columns:
		option_symbols = sorted(selected[symbol_column].dropna().astype(str).unique().tolist())
		if "underlying" in selected.columns:
			for future_symbol in future_symbols:
				future_mask = selected["underlying"].astype(str) == future_symbol
				option_symbols_by_future[future_symbol] = sorted(
					selected.loc[future_mask, symbol_column].dropna().astype(str).unique().tolist()
				)

	selected.to_parquet(opt_part_path, index=False)
	finalize_output(opt_part_path, opt_parquet_path)

	selected_futures.to_parquet(fut_part_path, index=False)
	finalize_output(fut_part_path, fut_parquet_path)

	selection = DefinitionSelection(
		day=day.isoformat(),
		query_start=utc_day_start(day).isoformat(),
		query_end=utc_day_end_exclusive(day).isoformat(),
		future_symbols=future_symbols,
		option_symbols=option_symbols,
		option_symbols_by_future=option_symbols_by_future,
		definition_rows=int(total_rows),
		output_parquet=str(opt_parquet_path),
		futures_output_parquet=str(fut_parquet_path),
	)
	opt_manifest_part_path.write_text(json.dumps(selection.__dict__, indent=2, sort_keys=True), encoding="utf-8")
	os.replace(opt_manifest_part_path, opt_manifest_path)
	fut_manifest_payload = {
		"day": day.isoformat(),
		"query_start": utc_day_start(day).isoformat(),
		"query_end": utc_day_end_exclusive(day).isoformat(),
		"future_symbols": future_symbols,
		"definition_rows": int(total_rows),
		"matched_future_definition_rows": int(len(selected_futures)),
		"output_parquet": str(fut_parquet_path),
	}
	fut_manifest_part_path.write_text(json.dumps(fut_manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
	os.replace(fut_manifest_part_path, fut_manifest_path)
	LOGGER.info(
		"filtered definitions for %s: matched %s option symbols and %s futures rows from %s definition rows",
		day.isoformat(),
		len(option_symbols),
		len(selected_futures),
		f"{total_rows:,}",
	)
	return selection


def load_definition_selection(manifest_path: Path) -> DefinitionSelection:
	manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
	manifest.setdefault("option_symbols_by_future", {})
	return DefinitionSelection(**manifest)


def sanitize_symbol_for_path(symbol: str) -> str:
	return symbol.replace("/", "-").replace(" ", "_")


def build_underlying_output_path(data_dir: Path, schema: str, day: date, future_symbol: str) -> Path:
	year = f"{day.year:04d}"
	month = f"{day.month:02d}"
	safe_symbol = sanitize_symbol_for_path(future_symbol)
	return data_dir / schema / year / month / f"{day.isoformat()}.{safe_symbol}.dbn.zst"


def build_option_output_path(
	data_dir: Path,
	schema: str,
	day: date,
	future_symbol: str,
	batch_index: int,
	total_batches: int,
) -> Path:
	year = f"{day.year:04d}"
	month = f"{day.month:02d}"
	safe_symbol = sanitize_symbol_for_path(future_symbol)
	return data_dir / schema / year / month / f"{day.isoformat()}.{safe_symbol}.batch-{batch_index + 1:04d}-of-{total_batches:04d}.dbn.zst"


def build_batch_manifest_path(out_path: Path) -> Path:
	return out_path.with_suffix(out_path.suffix + ".symbols.json")


def download_symbol_batch(
	api_key: str,
	dataset: str,
	schema: str,
	day: date,
	symbols: list[str],
	stype_in: str,
	out_path: Path,
	retries: int,
	retry_backoff_seconds: float,
	skip_existing: bool,
) -> Path | None:
	out_path.parent.mkdir(parents=True, exist_ok=True)
	batch_manifest_path = build_batch_manifest_path(out_path)
	if skip_existing and out_path.exists() and marker_path(out_path).exists() and batch_manifest_path.exists():
		LOGGER.info("skip existing data batch: %s", out_path)
		return out_path

	client = create_client(api_key)
	window = clamp_query_window(client, dataset, schema, utc_day_start(day), utc_day_end_exclusive(day))
	if window is None:
		LOGGER.warning("no %s window available for %s", schema, day.isoformat())
		return None
	query_start, query_end = window

	part_path = Path(f"{out_path}.part")
	manifest_part_path = Path(f"{batch_manifest_path}.part")
	remove_if_exists(part_path)
	remove_if_exists(marker_path(out_path))
	remove_if_exists(manifest_part_path)

	last_error = None
	for attempt in range(1, retries + 1):
		try:
			client.timeseries.get_range(
				dataset=dataset,
				schema=schema,
				symbols=symbols,
				stype_in=stype_in,
				start=query_start.isoformat(),
				end=query_end.isoformat(),
				path=str(part_path),
			)
			finalize_output(part_path, out_path)
			payload = {
				"schema": schema,
				"day": day.isoformat(),
				"symbols": symbols,
				"query_start": query_start.isoformat(),
				"query_end": query_end.isoformat(),
				"output_path": str(out_path),
			}
			manifest_part_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
			os.replace(manifest_part_path, batch_manifest_path)
			LOGGER.info("downloaded %s batch for %s -> %s", schema, day.isoformat(), out_path)
			return out_path
		except Exception as exc:
			last_error = exc
			remove_if_exists(part_path)
			if attempt == retries:
				break
			time.sleep(retry_backoff_seconds * attempt)

	raise RuntimeError(
		f"failed to download {schema} data for {day.isoformat()} batch size={len(symbols)}: {last_error}"
	) from last_error


def cleanup_residual_outputs(root_dirs: list[Path]) -> tuple[int, int]:
	removed_parts = 0
	removed_orphan_markers = 0
	seen_roots: set[Path] = set()

	for root_dir in root_dirs:
		normalized_root = root_dir.resolve()
		if normalized_root in seen_roots or not normalized_root.exists():
			continue
		seen_roots.add(normalized_root)

		for current_root, _, files in os.walk(normalized_root):
			for filename in files:
				file_path = Path(current_root) / filename
				if filename.endswith(".part"):
					remove_if_exists(file_path)
					removed_parts += 1
					continue
				if filename.endswith(".ok"):
					final_path = Path(str(file_path)[:-3])
					if not final_path.exists():
						remove_if_exists(file_path)
						removed_orphan_markers += 1

	return removed_parts, removed_orphan_markers


def process_definition_day(args: argparse.Namespace, db, day: date) -> DefinitionSelection | None:
	api_key = ensure_api_key(args.api_key)
	client = create_client(api_key)
	cache_path = definition_cache_path(Path(args.definition_cache_dir), day)
	opt_filtered_path, opt_manifest_path, fut_filtered_path, fut_manifest_path = definition_filtered_paths(Path(args.definition_filter_dir), day)
	dbn_path = download_definition_snapshot(
		client=client,
		dataset=args.dataset,
		day=day,
		out_path=cache_path,
		retries=args.retries,
		retry_backoff_seconds=args.retry_backoff_seconds,
		skip_existing=args.skip_existing,
	)
	if dbn_path is None:
		return None
	return filter_definition_snapshot(
		db=db,
		day=day,
		dbn_path=dbn_path,
		future_symbols=parse_csv_arg(args.futures),
		trading_classes=parse_csv_arg(args.trading_classes),
		strike_min=args.strike_min,
		strike_max=args.strike_max,
		chunk_rows=args.definition_chunk_rows,
		opt_parquet_path=opt_filtered_path,
		opt_manifest_path=opt_manifest_path,
		fut_parquet_path=fut_filtered_path,
		fut_manifest_path=fut_manifest_path,
		skip_existing=args.skip_existing,
	)


def run_definition_phase(args: argparse.Namespace, db) -> list[DefinitionSelection]:
	days = resolve_date_range(args.start_date, args.end_date)
	workers = max(1, min(args.workers, len(days)))
	LOGGER.info("starting definition phase for %s day(s) with %s worker(s)", len(days), workers)

	selections: list[DefinitionSelection] = []
	progress = create_progress(total=len(days), desc="definitions", enabled=args.progress)
	with ThreadPoolExecutor(max_workers=workers) as executor:
		future_to_day = {executor.submit(process_definition_day, args, db, day): day for day in days}
		for future in as_completed(future_to_day):
			day = future_to_day[future]
			try:
				selection = future.result()
			except Exception as exc:
				progress.close()
				raise RuntimeError(f"definition task failed for day={day.isoformat()}") from exc
			if selection is not None:
				selections.append(selection)
			progress.update(1)
	progress.close()

	selections.sort(key=lambda selection: selection.day)
	return selections


def load_all_definition_selections(definition_filter_dir: Path, days: list[date]) -> list[DefinitionSelection]:
	selections: list[DefinitionSelection] = []
	for day in days:
		_, manifest_path, _, _ = definition_filtered_paths(definition_filter_dir, day)
		if manifest_path.exists():
			selections.append(load_definition_selection(manifest_path))
	return selections


def run_data_phase(args: argparse.Namespace, selections: list[DefinitionSelection]) -> list[Path]:
	api_key = ensure_api_key(args.api_key)
	schemas = parse_csv_arg(args.schemas)
	underlying_data_dir = Path(args.underlying_data_dir)
	option_data_dir = Path(args.option_data_dir)
	workers = max(1, args.workers)
	batch_size = max(1, args.symbol_batch_size)
	LOGGER.info("starting data phase for %s selection day(s) with %s worker(s)", len(selections), workers)

	tasks = []
	for selection in selections:
		day = date.fromisoformat(selection.day)
		option_symbols_by_future = selection.option_symbols_by_future or {}
		for schema in schemas:
			for future_symbol in selection.future_symbols:
				underlying_out_path = build_underlying_output_path(underlying_data_dir, schema, day, future_symbol)
				tasks.append((schema, day, [future_symbol], "raw_symbol", underlying_out_path, "underlying", future_symbol))

				option_symbols = option_symbols_by_future.get(future_symbol, [])
				if not option_symbols:
					LOGGER.warning("no option symbols selected for %s %s; skipping option download", selection.day, future_symbol)
					continue
				batches = list(chunked(option_symbols, batch_size))
				total_batches = len(batches)
				for batch_index, batch_symbols in enumerate(batches):
					out_path = build_option_output_path(option_data_dir, schema, day, future_symbol, batch_index, total_batches)
					tasks.append((schema, day, batch_symbols, "raw_symbol", out_path, "option", future_symbol))

	if not tasks:
		LOGGER.warning("no data download tasks generated")
		return []

	LOGGER.info("generated %s download task(s)", len(tasks))

	completed_paths: list[Path] = []
	progress = create_progress(total=len(tasks), desc="downloads", enabled=args.progress)
	with ThreadPoolExecutor(max_workers=workers) as executor:
		future_to_task = {
			executor.submit(
				download_symbol_batch,
				api_key,
				args.dataset,
				schema,
				day,
				batch_symbols,
				stype_in,
				out_path,
				args.retries,
				args.retry_backoff_seconds,
				args.skip_existing,
			): (schema, day, batch_symbols, stype_in, out_path, instrument_kind, future_symbol)
			for schema, day, batch_symbols, stype_in, out_path, instrument_kind, future_symbol in tasks
		}
		for future in as_completed(future_to_task):
			schema, day, batch_symbols, stype_in, out_path, instrument_kind, future_symbol = future_to_task[future]
			try:
				result = future.result()
			except Exception as exc:
				progress.close()
				raise RuntimeError(
					f"download task failed for kind={instrument_kind} future={future_symbol} schema={schema} stype_in={stype_in} day={day.isoformat()} batch_size={len(batch_symbols)} out={out_path}"
				) from exc
			if result is not None:
				completed_paths.append(result)
			progress.update(1)
			progress.set_postfix_str(f"done={len(completed_paths)}/{len(tasks)}")
	progress.close()

	return completed_paths


def main() -> None:
	parser = build_parser()
	args = parser.parse_args()
	log_path = setup_logging(Path(args.log_dir), args.log_level)

	db = load_databento()
	if not args.start_date or not args.end_date:
		raise RuntimeError("--start-date and --end-date must be provided")
	future_symbols = parse_csv_arg(args.futures)
	if not future_symbols:
		raise RuntimeError("--futures must contain at least one futures symbol")
	if args.strike_min is not None and args.strike_max is not None and args.strike_min > args.strike_max:
		raise RuntimeError("--strike-min cannot be greater than --strike-max")

	definition_cache_dir = Path(args.definition_cache_dir)
	definition_filter_dir = Path(args.definition_filter_dir)
	underlying_data_dir = Path(args.underlying_data_dir)
	option_data_dir = Path(args.option_data_dir)
	for directory in (Path(args.output_root), definition_cache_dir, definition_filter_dir, underlying_data_dir, option_data_dir):
		directory.mkdir(parents=True, exist_ok=True)

	LOGGER.info("log file: %s", log_path)
	LOGGER.info(
		"run config: mode=%s dataset=%s futures=%s date_range=%s..%s schemas=%s workers=%s progress=%s",
		args.mode,
		args.dataset,
		parse_csv_arg(args.futures),
		args.start_date,
		args.end_date,
		parse_csv_arg(args.schemas),
		args.workers,
		args.progress,
	)

	removed_parts, removed_markers = cleanup_residual_outputs(
		[definition_cache_dir, definition_filter_dir, underlying_data_dir, option_data_dir]
	)
	if removed_parts or removed_markers:
		LOGGER.info("cleanup: removed %s .part files and %s orphan .ok markers", removed_parts, removed_markers)

	days = resolve_date_range(args.start_date, args.end_date)

	if args.mode in {"scan-definitions", "both"}:
		selections = run_definition_phase(args, db)
	else:
		selections = load_all_definition_selections(definition_filter_dir, days)

	LOGGER.info("loaded %s day-level definition selection(s)", len(selections))
	total_option_symbols = sum(len(selection.option_symbols) for selection in selections)
	LOGGER.info("total selected option symbols across all days: %s", total_option_symbols)

	if args.mode in {"download-data", "both"}:
		completed = run_data_phase(args, selections)
		LOGGER.info("completed %s data batch file(s)", len(completed))


if __name__ == "__main__":
	main()
