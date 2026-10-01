import logging
import databento as db
import polars as pl
from pathlib import Path
from typing import Iterator

from config import RAW_FUT_OPT_DIR, RAW_FUT_UND_DIR, STAGE1_DIR, CHUNK_SIZE_ROWS, get_processed_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def stream_dbn_to_parquet(dbn_path: Path, output_dir: Path, source_name: str) -> None:
    """将 DBN 文件按块流式写出 Parquet，避免内存爆炸"""
    try:
        # Databento's DBNStore lets us iterate dataframes by counts
        store = db.DBNStore.from_file(str(dbn_path))

        # 1. Gather Metadata for validation
        md = store.metadata
        logger.info(f"Processing DBN: {dbn_path.name} | Schema: {md.schema}")

        part_idx = 0
        for pdf in store.to_df(count=CHUNK_SIZE_ROWS):
            if pdf is None or pdf.empty:
                continue

            # 2. DataFrame standardization via Polars
            df = pl.from_pandas(pdf.reset_index())

            # Common standardization defined in data_flow.ipynb
            # Ensure proper names for timestamps, usually 'ts_recv' is the index or regular column
            if 'ts_recv' in df.columns:
                df = df.rename({'ts_recv': 'ts_recv_ns'})
            if 'ts_event' in df.columns:
                df = df.rename({'ts_event': 'ts_event_ns'})

            df = df.with_columns([
                pl.lit(source_name).alias("source_file"),
                pl.lit("v1.0").alias("stage_version")
            ])

            # Write parquet block
            part_path = output_dir / f"{dbn_path.stem}.part{part_idx:04d}.parquet"
            df.write_parquet(part_path, compression="zstd")
            logger.debug(f"Wrote fraction {part_idx} to {part_path.name} (Rows: {len(df)})")
            part_idx += 1

        logger.info(f"Finished writing {part_idx} parquet parts for {dbn_path.name}")

    except Exception as e:
        logger.error(f"Error processing {dbn_path}: {e}", exc_info=True)

def run_stage1_normalization(trade_dates: list[str]) -> None:
    """
    遍历给定日期的原始数据文件，并将 DBN 转存为 Parquet 以便于下游 Lazy 引擎使用。
    """
    logger.info(f"--- Starting Stage 1 DBN -> Parquet Normalization for dates: {trade_dates} ---")

    # 待处理任务清单
    tasks = []

    # 构建所有待扫目录和策略
    scan_configs = [
        {'asset': 'opt', 'schema': 'mbp-10', 'root': RAW_FUT_OPT_DIR / 'mbp-10'},
        {'asset': 'opt', 'schema': 'trades', 'root': RAW_FUT_OPT_DIR / 'trades'},
        {'asset': 'und', 'schema': 'mbp-10', 'root': RAW_FUT_UND_DIR / 'mbp-10'},
        {'asset': 'und', 'schema': 'trades', 'root': RAW_FUT_UND_DIR / 'trades'},
    ]

    for date_str in trade_dates:
        # e.g., "2026-05-10"
        y, m, d = date_str.split('-')

        for sc in scan_configs:
            # Databento folder structure is heavily year/month nested in original script
            # Example path: root/<year>/<month>/*.dbn.zst (if typical structure)
            # In our provided tree, it's just raw_db_und / ...
            # We'll deeply glob for the date string in the filename
            target_files = list(sc['root'].rglob(f"*{date_str}*.dbn.zst"))

            out_dir = get_processed_path(STAGE1_DIR, sc['asset'], sc['schema'], date_str)

            for file_path in target_files:
                # 检查是否已经跑过（是否存在对应的 parquet 文件）
                part_path = out_dir / f"{file_path.stem}.part0000.parquet"
                if part_path.exists():
                    logger.info(f"Skipping Stage 1 for {file_path.name} as it already exists in {out_dir}")
                    continue
                tasks.append((file_path, out_dir, file_path.name))

    # 单机按序或多进程执行
    # 为了防止进程占用过高内存，这里以简单的顺序执行，依赖 Databento 内部以及 Polars 的 C/Rust 释放内存
    # 也可根据环境替换为 ThreadPoolExecutor
    for dbn_path, output_dir, source_name in tasks:
        stream_dbn_to_parquet(dbn_path, output_dir, source_name)

if __name__ == "__main__":
    import sys
    dates = sys.argv[1:] if len(sys.argv) > 1 else ["2026-05-12"]
    run_stage1_normalization(dates)
