#!/usr/bin/env python3
"""验证 Databento SDK 默认输出的格式"""
import sys
import databento as db
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import RAW_FUT_UND_DIR

# 查找一个原始 DBN 文件
dbn_files = list(RAW_FUT_UND_DIR.rglob('*.dbn.zst'))
if len(dbn_files) == 0:
    print("找不到原始 DBN 文件")
else:
    test_file = dbn_files[0]
    print(f"读取原始 DBN 验证: {test_file}")
    store = db.DBNStore.from_file(str(test_file))
    print(f"Metadata Schema: {store.metadata.schema}")

    # 读取前 5 行
    pdf = next(store.to_df(count=5))
    print("\n前 5 行原始 Pandas DataFrame:")
    print(pdf.head())
    print("\n数据类型:")
    print(pdf.dtypes)
