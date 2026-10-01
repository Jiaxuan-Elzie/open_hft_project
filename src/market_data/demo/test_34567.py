import sys
import logging

from stage3_sessionize import run_stage3_sessionize
from stage4_clean_trades import run_stage4_clean
from stage5_clean_mbp10 import run_stage5_clean
from stage6_align import run_stage6_align
from stage7_qc import run_stage7_qc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    dates = ["2026-05-11"]

    logger.info(">>> STEP 3: Timezone / Session mapping")
    run_stage3_sessionize(dates)

    logger.info(">>> STEP 4: Cleaning Trades")
    run_stage4_clean(dates)

    logger.info(">>> STEP 5: Cleaning MBP-10 Books")
    run_stage5_clean(dates)

    logger.info(">>> STEP 6: AsOf Alignment (Trade vs Book)")
    run_stage6_align(dates)

    logger.info(">>> STEP 7: Quality Control Reporting")
    run_stage7_qc(dates)
