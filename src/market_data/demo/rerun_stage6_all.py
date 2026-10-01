import sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from stage6_align import align_trade_book
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 所有可用日期
all_dates = [
    "2026-05-10",
    "2026-05-11",
    "2026-05-12",
    "2026-05-13",
    "2026-05-14",
    "2026-05-15",
    "2026-05-17",
    "2026-05-18",
    "2026-05-19",
    "2026-05-20",
    "2026-05-21",
    "2026-05-22",
    "2026-05-24",
    "2026-05-25",
    "2026-05-26",
    "2026-05-27"
]

# 所有资产类型
assets = ['und', 'opt']

for date_str in all_dates:
    for asset in assets:
        print(f"\n--- Processing {date_str} - {asset} ---")
        align_trade_book(date_str, asset)
