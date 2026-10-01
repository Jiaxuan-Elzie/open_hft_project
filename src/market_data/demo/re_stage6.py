import logging
import sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from stage6_align import align_trade_book

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 重新运行 Stage6
date_str = '2026-05-12'
align_trade_book(date_str, 'und')
