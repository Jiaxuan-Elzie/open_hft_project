# Open HFT Project

高频市场数据抓取、增量更新和清洗示例。仓库包含两条数据抓取链路，以及一套针对 Databento 期货与期权 DBN 数据的 Stage 1-7 处理流程。

仓库只包含代码、schema 和少量参考配置，不包含原始行情、处理结果、日志或访问凭据。

## English Overview

This repository presents a research-oriented pipeline for high-frequency futures, options, and equity market data. It contains two resumable data-acquisition workflows—Databento futures/options and Massive equity/OPRA flat files—and a seven-stage processing pipeline covering normalization, contract symbology, session construction, trade and MBP-10 cleaning, futures-option alignment, and quality-control reporting.

The public repository is intended to demonstrate the data engineering, temporal alignment, and validation infrastructure used to prepare market-microstructure research datasets. It includes source code, schemas, and limited reference configuration only. Licensed raw data, processed research outputs, execution logs, internal notes, and credentials are not distributed. Access credentials are read from environment variables and should never be committed to Git.

The published acquisition paths and Stage 1-7 pipeline are the maintained public workflow. Running the complete pipeline requires the relevant data subscriptions and local data files; the repository does not claim that licensed datasets can be reproduced without those services.

## 项目结构

```text
src/
├── data_fetch/
│   ├── fut/                 # Databento 期货与期权抓取、守护进程
│   └── stock/               # Massive 股票与 OPRA 期权 flatfiles 抓取
└── market_data/
    ├── demo/                # Stage 1-7 清洗、对齐与 QC pipeline
    └── *.md                 # 数据清洗设计文档
```

## 安装

建议使用 Python 3.10 或更高版本。

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

配置数据服务凭据。凭据只从环境变量读取，不要写入代码或提交到 Git：

```bash
export DATABENTO_API_KEY="..."
export MASSIVE_AWS_KEY="..."
export MASSIVE_AWS_SECRET="..."
```

默认数据目录位于仓库根目录下的 `data/`，运行日志位于 `logs/`。这些目录已被 `.gitignore` 排除。

## 数据抓取流程

### 1. Databento 期货与期权

入口：`src/data_fetch/fut/raw_data_fetch.py`

流程分为两阶段：

1. 按日期下载 `definition` 快照。
2. 按期货代码、交易类别和行权价范围筛选期权定义，生成每日 futures/options manifest。
3. 根据 manifest 下载期货和期权的 `trades`、`mbp-10` 等 schema。
4. 数据先写入 `.part` 文件，成功后原子重命名，并写入 `.ok` 完成标记。
5. 再次运行时默认跳过带 `.ok` 的完整结果，实现按日、按 schema、按 symbol batch 的增量更新。

完整抓取示例：

```bash
python src/data_fetch/fut/raw_data_fetch.py \
  --start-date 2026-05-10 \
  --end-date 2026-05-12 \
  --futures GCM6,GCQ6,GCV6 \
  --schemas trades,mbp-10 \
  --mode both
```

仅刷新定义或仅根据已有 manifest 下载行情：

```bash
python src/data_fetch/fut/raw_data_fetch.py --mode scan-definitions --start-date 2026-05-10 --end-date 2026-05-12
python src/data_fetch/fut/raw_data_fetch.py --mode download-data --start-date 2026-05-10 --end-date 2026-05-12
```

守护进程会在异常退出后重启抓取任务：

```bash
python src/data_fetch/fut/guard_raw_data_fetch.py start -- \
  --start-date 2026-05-10 --end-date 2026-05-12 --mode both
python src/data_fetch/fut/guard_raw_data_fetch.py status
python src/data_fetch/fut/guard_raw_data_fetch.py tail --follow
python src/data_fetch/fut/guard_raw_data_fetch.py stop
```

默认输出：

```text
data/raw_data/fut/
├── definition_cache/        # 原始 definition DBN
├── definition_filtered/     # 筛选后的 parquet 与 manifest
├── raw_db_und/              # 期货行情
└── raw_db_opt/              # 期权行情
```

### 2. Massive 股票与 OPRA 期权

期权入口：`src/data_fetch/stock/test_minimum_option_quotes.py`

1. 从 Massive S3-compatible flatfiles 按日期下载 OPRA `trades` 或 `quotes`。
2. 使用分段缓存和 `.part`/`.ok` 标记支持中断恢复。
3. 可在流式读取时只保留 S&P 500 标的，避免整文件载入内存。
4. 从期权 ticker 提取唯一 underlying 列表，供股票数据更新使用。

```bash
python src/data_fetch/stock/test_minimum_option_quotes.py \
  --start-date 2026-05-01 --end-date 2026-05-02 \
  --feed quotes --mode both
```

股票入口：`src/data_fetch/stock/test_minimum_und.py`

1. 读取 underlying CSV 或仓库内的 S&P 500 symbol 表。
2. 按日期下载 SIP `trades` 或 `quotes` flatfile。
3. 分块解压并过滤目标 ticker，写入按 feed/date 分区的压缩 CSV。
4. 已存在且带 `.ok` 标记的日期默认跳过，因此日常更新只处理缺失日期。

```bash
python src/data_fetch/stock/test_minimum_und.py \
  --start-date 2026-05-01 --end-date 2026-05-02 \
  --feed quotes
```

股票与期权下载器也提供 `monitor_*` 和 `guard_*` 脚本，用于后台运行、状态查询、日志查看和网络流错误后的自动重启。

## 数据更新与清洗

入口：`src/market_data/demo/run_pipeline.py`

```bash
cd src/market_data/demo
python run_pipeline.py --dates 2026-05-10,2026-05-11
```

处理链路：

| Stage | 输入 | 处理 | 主要输出 |
|---|---|---|---|
| 1 Normalize | 原始 DBN | 分块读取、统一时间字段、写 zstd parquet | `stage1_parquet` |
| 2 Symbology | Stage 1 + definition | 映射 instrument、合约、到期日和标的 | `stage2_symbology` |
| 3 Sessionize | Stage 1 + Stage 2 | 挂接合约语义、交易时段、延迟和记录 ID | `stage3_sessionized` |
| 4 Clean Trades | Stage 3 trades | 标记无效价格、数量、时间、方向和延迟 | `stage4_clean_trades` |
| 5 Clean MBP-10 | Stage 3 books | 计算 top-of-book、mid、spread，标记 crossed/sparse/wide book | `stage5_clean_mbp10` |
| 6 Align | Stage 4 + Stage 5 | 以期货 MBP 为锚点建立期货/期权链索引及 as-of 对齐 | `stage6_aligned` |
| 7 QC | Stage 5 + Stage 6 | 汇总清洗率、盘口质量、延迟和对齐质量 | `stage7_qc_reports` |

Stage 4 和 Stage 5 在数据依赖上可并行，但当前 `run_pipeline.py` 按顺序执行。Stage 6 同时保留锚点索引层和兼容的 `aligned_trade_book.parquet`，Stage 7 使用后者生成 QC 汇总。

默认处理目录：

```text
data/processed/
├── stage1_parquet/
├── stage2_symbology/
├── stage3_sessionized/
├── stage4_clean_trades/
├── stage5_clean_mbp10/
├── stage6_aligned/
└── stage7_qc_reports/
```

## 增量更新建议

1. 每次更新使用明确的 `--start-date` 和 `--end-date`。
2. 保留原始抓取目录中的 `.ok` 标记；它们是断点续传和幂等跳过的依据。
3. Databento 新交易日先运行 definition scan，再下载行情。
4. Massive 先更新期权并提取 underlying，再更新对应股票 feed。
5. 对新增交易日运行 Stage 1-7，不要把不同 Stage 版本的 parquet 混在同一分区。
6. 发布数据前检查 Stage 7 的 `publishable` 和各项 `chk_pass_*` 指标。

更详细的字段规则和清洗设计见 `src/market_data/data_cleaning_fut.md`、`src/market_data/data_cleaning_fut_interface.md` 与 `src/market_data/data_cleaning.md`。

## 公开版本说明

公开版本仅保留当前维护的数据抓取入口和 Stage 1-7 处理链路。旧工程中未被公开流程使用的 legacy downloader 未纳入仓库，以避免发布不完整的内部依赖。带执行输出的 notebook、授权行情数据、运行日志、缓存文件和内部会议记录同样未纳入公开版本。

本仓库用于展示研究数据基础设施和质量控制方法。完整运行需要相应的数据服务权限；公开仓库本身不分发或替代授权数据源。
