#!/usr/bin/env python3
"""测试 Databento SDK 的 to_ndarray 方法，看是否返回纳元整数格式"""
import sys
import numpy as np
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

    # 使用 to_ndarray 方法，这可能会保留原始数据类型！
    # 不传入 count，直接获取整个 array
    arr = store.to_ndarray()
    print("\n使用 to_ndarray 读取的原始 NumPy array:")
    print(arr)
    print(f"\n数据类型 (dtype): {arr.dtype}")
    print("\n字段信息:")
    for name in arr.dtype.names:
        print(f"  {name:<16} {arr.dtype[name]}")
