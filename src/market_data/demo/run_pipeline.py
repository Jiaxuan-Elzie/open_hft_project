import logging
import argparse
from stage1_normalize import run_stage1_normalization
from stage2_symbology import run_stage2_symbology
from stage3_sessionize import run_stage3_sessionize
from stage4_clean_trades import run_stage4_clean
from stage5_clean_mbp10 import run_stage5_clean
from stage6_align import run_stage6_align
from stage7_qc import run_stage7_qc
from config import RAW_FUT_OPT_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_all(trade_dates: list[str]):
    """执行完整的 Pipeline 流水线并打印进度"""

    # 步骤 1: DBN -> Parquet (支持断点、释放内存)
    logger.info(">>> STEP 1: DBN to Parquet Normalization")
    run_stage1_normalization(trade_dates)

    # 步骤 2: 生成合约和执行价/底仓的联动挂载关系 Symbology
    logger.info(">>> STEP 2: Building Symbology Table")
    run_stage2_symbology(trade_dates)

    # 步骤 3: 按时间打 session 以及延迟表
    logger.info(">>> STEP 3: Timezone / Session mapping")
    run_stage3_sessionize(trade_dates)

    # 步骤 4+5 (并行/无相互顺序依赖): 过滤单维度中的异常
    logger.info(">>> STEP 4: Cleaning Trades")
    run_stage4_clean(trade_dates)

    logger.info(">>> STEP 5: Cleaning MBP-10 Books")
    run_stage5_clean(trade_dates)

    # 步骤 6: join_asof 主体对齐拼接
    logger.info(">>> STEP 6: AsOf Alignment (Trade vs Book)")
    run_stage6_align(trade_dates)

    # 步骤 7: 输出最终 QA / QC 指标汇总
    logger.info(">>> STEP 7: Quality Control Reporting")
    run_stage7_qc(trade_dates)

    logger.info(">>> PIPELINE COMPLETED SUCCESSFULLY <<<")

import sys
from pathlib import Path

def get_available_dates() -> list[str]:
    """扫描 raw_db_opt 目录下的 zst 文件提取可用的交易日期。"""
    dates = set()
    raw_dir = RAW_FUT_OPT_DIR
    if raw_dir.exists():
        for file_path in raw_dir.glob("*.zst"):
            # 文件名格式例如：2026-05-10.futures.dbn.zst 或者 2026-05-10.GCQ6.dbn.zst
            parts = file_path.name.split('.')
            if len(parts) >= 2:
                dates.add(parts[0])
    return sorted(list(dates))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HFT Data Cleaning Pipeline Demo")
    parser.add_argument("--dates", type=str, help="Comma separated dates e.g. 2026-05-10,2026-05-11. If not provided, scans raw_db_opt for all dates.", default="")
    args = parser.parse_args()

    if args.dates:
        dates_list = [d.strip() for d in args.dates.split(',')]
    else:
        dates_list = get_available_dates()
        if not dates_list:
            logger.error("No dates provided and no data found in raw_db_opt.")
            sys.exit(1)
        logger.info(f"Auto-detected dates to process: {dates_list}")

    logger.info(f"Starting pipeline for dates: {dates_list}")
    run_all(dates_list)