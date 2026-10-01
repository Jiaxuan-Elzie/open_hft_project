import os
import re
from pathlib import Path

# ---------- PATHS ----------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DATA_ROOT = PROJECT_ROOT / 'data' / 'raw_data' / 'fut'
PROCESSED_ROOT = PROJECT_ROOT / 'data' / 'processed'
TRADING_CALENDAR_DIR = PROJECT_ROOT / 'data' / 'trading_calendar'

# 源数据目录
RAW_FUT_OPT_DIR = RAW_DATA_ROOT / 'raw_db_opt'
RAW_FUT_UND_DIR = RAW_DATA_ROOT / 'raw_db_und'
DEF_FILTER_DIR = RAW_DATA_ROOT / 'definition_filtered'
DEF_FILTER_OPT_DIR = DEF_FILTER_DIR / 'opt'
DEF_FILTER_FUT_DIR = DEF_FILTER_DIR / 'fut'

# 输出目录
STAGE1_DIR = PROCESSED_ROOT / 'stage1_parquet'
STAGE2_DIR = PROCESSED_ROOT / 'stage2_symbology'
STAGE3_DIR = PROCESSED_ROOT / 'stage3_sessionized'
STAGE4_DIR = PROCESSED_ROOT / 'stage4_clean_trades'
STAGE5_DIR = PROCESSED_ROOT / 'stage5_clean_mbp10'
STAGE6_DIR = PROCESSED_ROOT / 'stage6_aligned'
STAGE7_DIR = PROCESSED_ROOT / 'stage7_qc_reports'

# ---------- CORE CONSTANTS ----------
PRICE_UNIT = 1e9  # Databento default multiplier
CHUNK_SIZE_ROWS = 500_000 # Default DBN chunk reading to avoid OOM

# Mappings to normalize datasets
ASSET_TYPES = ['und', 'opt']
SCHEMAS = ['trades', 'mbp-10']

# Calendar & Timezone
NY_TIMEZONE = "America/New_York"
RTH_START_TIME = "09:30:00"
RTH_END_TIME = "16:00:00"

# Product root -> exchange calendar mapping.
# Extend this map as more underlyings are added.
PRODUCT_CALENDAR_MAP = {
    "GC": {
        "calendar_name": "CMEGlobex_GC",
        "exchange": "CME",
        "session_type": "globex",
    },
}

DEFAULT_CALENDAR_CONFIG = {
    "calendar_name": "NYSE",
    "exchange": "UNKNOWN",
    "session_type": "cash",
}

_MONTH_CODE_PATTERN = re.compile(r"^([A-Z]+?)(?=[FGHJKMNQUVXZ]\d)")


def extract_product_root(symbol: str | None) -> str | None:
    """Extract the product root from symbols such as GCM6, GCQ6 or OGM6 C3700 via the underlying."""
    if not symbol:
        return None
    candidate = symbol.strip().split()[0]
    match = _MONTH_CODE_PATTERN.match(candidate)
    if not match:
        return None
    return match.group(1)


def resolve_calendar_config(product_root: str | None) -> dict:
    if not product_root:
        return DEFAULT_CALENDAR_CONFIG
    return PRODUCT_CALENDAR_MAP.get(product_root, DEFAULT_CALENDAR_CONFIG)

def get_processed_path(stage_dir: Path, asset_type: str, schema: str, trade_date: str) -> Path:
    """标准化的输出分区路径管理工具。"""
    path = stage_dir / schema / asset_type / trade_date
    path.mkdir(parents=True, exist_ok=True)
    return path
