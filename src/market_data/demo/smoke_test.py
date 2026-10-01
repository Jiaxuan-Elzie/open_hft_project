import sys
import logging
from unittest.mock import patch
import databento as db
import polars as pl

# Import the modules
import stage1_normalize
import run_pipeline

original_stream = stage1_normalize.stream_dbn_to_parquet

def mock_stream_dbn_to_parquet(dbn_path, output_dir, source_name):
    # Only process up to 1 chunk
    try:
        store = db.DBNStore.from_file(str(dbn_path))
        md = store.metadata
        stage1_normalize.logger.info(f"SMOKE TEST Processing DBN: {dbn_path.name} | Schema: {md.schema}")

        part_idx = 0
        for pdf in store.to_df(count=1000): # Small chunk
            if pdf is None or pdf.empty:
                continue

            df = pl.from_pandas(pdf.reset_index())

            if 'ts_recv' in df.columns:
                df = df.rename({'ts_recv': 'ts_recv_ns'})
            if 'ts_event' in df.columns:
                df = df.rename({'ts_event': 'ts_event_ns'})

            df = df.with_columns([
                pl.lit(source_name).alias("source_file"),
                pl.lit("v1.0").alias("stage_version")
            ])

            part_path = output_dir / f"{dbn_path.stem}.part{part_idx:04d}.parquet"
            df.write_parquet(part_path, compression="zstd")
            part_idx += 1

            # BREAK AFTER 1 CHUNK FOR SMOKE TEST
            break

        stage1_normalize.logger.info(f"SMOKE TEST Finished writing {part_idx} parquet parts for {dbn_path.name}")

    except Exception as e:
        stage1_normalize.logger.error(f"Error processing {dbn_path}: {e}", exc_info=True)

def mock_run_stage1_normalization(trade_dates):
    stage1_normalize.logger.info(f"SMOKE TEST --- Starting Stage 1 DBN -> Parquet Normalization for dates: {trade_dates} ---")
    tasks = []
    scan_configs = [
        {'asset': 'opt', 'schema': 'mbp-10', 'root': stage1_normalize.RAW_FUT_OPT_DIR / 'mbp-10'},
        {'asset': 'opt', 'schema': 'trades', 'root': stage1_normalize.RAW_FUT_OPT_DIR / 'trades'},
        {'asset': 'und', 'schema': 'mbp-10', 'root': stage1_normalize.RAW_FUT_UND_DIR / 'mbp-10'},
        {'asset': 'und', 'schema': 'trades', 'root': stage1_normalize.RAW_FUT_UND_DIR / 'trades'},
    ]
    for date_str in trade_dates:
        for sc in scan_configs:
            # Only 1 file per type
            target_files = list(sc['root'].rglob(f"*{date_str}*.dbn.zst"))[:1]
            out_dir = stage1_normalize.get_processed_path(stage1_normalize.STAGE1_DIR, sc['asset'], sc['schema'], date_str)
            for file_path in target_files:
                tasks.append((file_path, out_dir, file_path.name))

    for dbn_path, output_dir, source_name in tasks:
        mock_stream_dbn_to_parquet(dbn_path, output_dir, source_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with patch('run_pipeline.run_stage1_normalization', side_effect=mock_run_stage1_normalization):
        run_pipeline.run_all(["2026-05-11"])
