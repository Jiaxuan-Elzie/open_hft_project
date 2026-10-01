import logging
import os
import sys
import time
from pathlib import Path

import duckdb

from config import TRADING_CALENDAR_DIR
from stage1_normalize import run_stage1_normalization
from stage2_symbology import run_stage2_symbology
from stage3_sessionize import DUCKDB_MEMORY_LIMIT
from stage3_sessionize import run_stage3_sessionize
from stage4_clean_trades import run_stage4_clean
from stage5_clean_mbp10 import run_stage5_clean
from stage6_align import run_stage6_align
from stage7_qc import run_stage7_qc

LOG_DIR = Path(__file__).resolve().parent / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOG_DIR / "test_pipeline.log"

logger = logging.getLogger("TEST_PIPELINE")
logger.setLevel(logging.INFO)
logger.propagate = False

if logger.handlers:
    logger.handlers.clear()

log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(log_formatter)

file_handler = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(log_formatter)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)


def _log_runtime_config():
    duckdb_temp_dir = TRADING_CALENDAR_DIR.parent / "tmp" / "duckdb_stage3"
    logger.info(
        "Runtime config: duckdb_version=%s stage3_duckdb_memory_limit=%s stage3_duckdb_temp_dir=%s env_stage3_duckdb_memory_limit=%s log_file=%s",
        duckdb.__version__,
        DUCKDB_MEMORY_LIMIT,
        duckdb_temp_dir,
        os.getenv("STAGE3_DUCKDB_MEMORY_LIMIT"),
        LOG_FILE_PATH,
    )


def _run_stage(stage_no: int, stage_name: str, stage_func, dates):
    logger.info(f"[Stage {stage_no}] Starting {stage_name} for dates={dates}")
    stage_start_time = time.time()

    try:
        stage_func(dates)
    except Exception:
        elapsed = time.time() - stage_start_time
        logger.error(
            f"[Stage {stage_no}] Failed {stage_name} after {elapsed:.2f} seconds",
            exc_info=True,
        )
        raise

    elapsed = time.time() - stage_start_time
    logger.info(f"[Stage {stage_no}] Completed {stage_name} in {elapsed:.2f} seconds")


def _run_single_date_pipeline(test_date: str):
    logger.info(f"=== Starting Full Pipeline Test for {test_date} ===")
    start_time = time.time()

    dates = [test_date]

    try:
        _run_stage(1, "normalization", run_stage1_normalization, dates)
        _run_stage(2, "symbology", run_stage2_symbology, dates)
        _run_stage(3, "sessionize", run_stage3_sessionize, dates)
        _run_stage(4, "clean_trades", run_stage4_clean, dates)
        _run_stage(5, "clean_mbp10", run_stage5_clean, dates)
        _run_stage(6, "align", run_stage6_align, dates)
        _run_stage(7, "qc", run_stage7_qc, dates)

        elapsed = time.time() - start_time
        logger.info(f"=== Pipeline completed successfully for {test_date} in {elapsed:.2f} seconds ===")
    except Exception:
        elapsed = time.time() - start_time
        logger.error(f"=== Pipeline failed for {test_date} after {elapsed:.2f} seconds ===", exc_info=True)
        raise


def run_test_pipeline(test_dates: list[str]):
    if not test_dates:
        raise ValueError("test_dates must not be empty")

    logger.info(f"=== Starting Full Pipeline Test for {len(test_dates)} date(s): {test_dates} ===")
    pipeline_start_time = time.time()
    _log_runtime_config()

    for index, test_date in enumerate(test_dates, start=1):
        logger.info(f"=== Processing date {index}/{len(test_dates)}: {test_date} ===")
        _run_single_date_pipeline(test_date)

    elapsed = time.time() - pipeline_start_time
    logger.info(f"=== All pipeline dates completed successfully in {elapsed:.2f} seconds ===")


if __name__ == "__main__":
    input_dates = sys.argv[1:] if len(sys.argv) > 1 else [
        # "2026-05-10",
        # "2026-05-11",
        # "2026-05-12",
        # "2026-05-13",
        # "2026-05-14",
        # "2026-05-15",
        # "2026-05-16",
        "2026-05-17",
        "2026-05-18",
        "2026-05-19",
        "2026-05-20",
        "2026-05-21",
        "2026-05-22",
        # "2026-05-23",
        "2026-05-24",
        "2026-05-25",
        "2026-05-26",
        "2026-05-27",
    ]

    try:
        run_test_pipeline(input_dates)
    except Exception:
        sys.exit(1)
