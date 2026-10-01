# FUT Raw Data Stage Interface Spec

> 目标: 基于 `data/raw_data/fut` 的真实数据，定义一套可并行开发的 stage 接口契约。
> 原则: 每个 stage 只依赖上一个 stage 的发布结果，不跨层读原始 DBN，不共享进程内对象，只共享落盘产物和 manifest。

---

## 1. 原始输入数据观察

实际目录:

```text
data/raw_data/fut/
├── raw_db_opt/
│   ├── mbp-10/
│   └── trades/
└── raw_db_und/
    ├── mbp-10/
    └── trades/
```

实际样本:

- `raw_db_opt/mbp-10/2026/05/2026-05-27.GCV6.batch-0013-of-0013.dbn.zst`
- `raw_db_opt/trades/2026/05/2026-05-27.GCV6.batch-0012-of-0013.dbn.zst`
- `raw_db_und/mbp-10/2026/05/2026-05-27.GCV6.dbn.zst`
- `raw_db_und/trades/2026/05/2026-05-27.GCV6.dbn.zst`

实际字段观察:

### 1.1 `und_trades`

```text
ts_recv(index), ts_event, rtype, publisher_id, instrument_id,
action, side, depth, price, size, flags, ts_in_delta, sequence, symbol
```

示例:

```text
ts_recv=2026-05-27 00:00:21.507143387+00:00
ts_event=2026-05-27 00:00:21.506169635+00:00
action=T
side=N
price=4586.0
size=1
symbol=GCV6
```

### 1.2 `und_mbp-10`

```text
ts_recv(index), ts_event, rtype, publisher_id, instrument_id,
action, side, depth, price, size, flags, ts_in_delta, sequence,
bid_px_00..bid_px_09, ask_px_00..ask_px_09,
bid_sz_00..bid_sz_09, ask_sz_00..ask_sz_09,
bid_ct_00..bid_ct_09, ask_ct_00..ask_ct_09,
symbol
```

示例:

```text
symbol=GCV6
action=A/T/M
bid_px_00=4500.0
ask_px_00=4586.0
bid_sz_00=4
ask_sz_00=2
```

### 1.3 `fut_trades`

字段结构与 `und_trades` 相同，`symbol` 为期权合约，例如:

```text
symbol=OGV6 P4600
action=T
side=B
price=239.2
size=1
```

### 1.4 `fut_mbp-10`

字段结构与 `und_mbp-10` 相同，`symbol` 为期权合约，例如:

```text
symbol=OGV6 P5155
bid_px_00=485.3
ask_px_00=769.6
bid_sz_00=2
ask_sz_00=2
```

### 1.5 原始输入统一结论

- `und` 是标的期货主链数据，例如 `GCV6`。
- `fut` 目录名虽然叫 `raw_db_opt`，但实际承载的是期权链数据，symbol 形如 `OGV6 P5155`。
- 所有 DBN 输入都有一组共享核心字段: `ts_recv`, `ts_event`, `publisher_id`, `instrument_id`, `action`, `side`, `price`, `size`, `flags`, `sequence`, `symbol`。
- `mbp-10` 相比 `trades` 多了 10 档盘口字段。
- 因为原始文件存在空文件或空 batch，接口层必须允许 stage 0 输出 `is_empty_file` 和 `usable=false`。

---

## 2. 并行开发总原则

为了让不同阶段并行开发，接口必须固定以下约束:

1. 每个 stage 只读上一个 stage 的发布目录，不允许向前跨 stage 读数据。
2. 每个 stage 都输出两类文件:
   - 业务数据文件: `parquet`
   - 控制文件: `manifest.json`
3. 每个 stage 的输出目录按相同分区组织:

```text
{stage_root}/
  version={stage_version}/
    date=YYYY-MM-DD/
      asset_type=und|opt/
        schema=trades|mbp10|joined|qc/
          symbol_root=GCV6/
            part-*.parquet
            manifest.json
            _SUCCESS
```

4. 上下游对接只认 `manifest.json` 里的 schema 版本，不靠口头约定。
5. 所有时间戳列在 stage 间传递时统一使用 `int64 ns UTC`，展示层再转时区。
6. 所有价格列在 stage 间统一使用 `float64` 或 `int64 fixed-point` 二选一，不能混用。这里建议统一 `float64`，因为当前 Databento 样本读出后就是浮点价格。
7. 每条记录必须保留以下最小追溯字段:

```text
trade_date, asset_type, raw_schema, symbol, source_file,
publisher_id, instrument_id, sequence, ts_recv_ns, ts_event_ns
```

---

## 3. Stage 划分与接口定义

建议拆成 7 个 stage，边界如下。

### Stage 0: Raw File Scan

#### 目标

扫描原始 DBN 文件，建立可处理文件清单，过滤空文件、坏文件、缺失文件。

#### 输入

- 原始目录 `data/raw_data/fut/raw_db_opt/*`
- 原始目录 `data/raw_data/fut/raw_db_und/*`

#### 处理方法

1. 遍历 `raw_db_opt/{mbp-10,trades}` 和 `raw_db_und/{mbp-10,trades}`。
2. 解析文件名，抽取 `trade_date`, `symbol_root`, `batch_id`, `asset_type`, `schema`。
3. 尝试打开 DBN 文件并读取前 1 条记录。
4. 记录以下状态:
   - `exists`
   - `can_open`
   - `is_empty_file`
   - `usable`
5. 输出每个日期、每个 symbol_root、每个 schema 的原始文件 manifest。

#### 输出格式

输出文件: `schema=file_manifest`

建议列:

```text
trade_date: date
asset_type: string         # und | opt
schema: string             # trades | mbp10
symbol_root: string        # GCV6
file_name: string
source_file: string
batch_id: string | null
file_size_bytes: int64
exists: bool
can_open: bool
is_empty_file: bool
usable: bool
error_code: string | null
error_message: string | null
stage_version: string
created_at_ns: int64
```

#### 并行边界

- 下游只依赖 `usable=true` 的 manifest，不再自己扫原始目录。

---

### Stage 1: Raw Normalize

#### 目标

把 DBN 原始记录拉平成统一表结构，形成标准化 trades / mbp10 两类底表。

#### 输入

- Stage 0 `file_manifest`

#### 处理方法

1. 对 `usable=true` 的文件逐个读取。
2. 统一重命名和补列。
3. 把 `ts_recv` 和 `ts_event` 转成 `ts_recv_ns`, `ts_event_ns`。
4. `mbp-10` 输出完整 10 档字段。
5. 增加公共元信息列:
   - `trade_date`
   - `asset_type`
   - `raw_schema`
   - `symbol_root`
   - `source_file`

#### 输出格式

输出 1: `schema=trades`

```text
trade_date: date
asset_type: string
raw_schema: string         # trades
symbol_root: string
symbol: string
publisher_id: int32
instrument_id: int64
action: string
side: string
depth: int32
price: float64
size: int64
flags: int64
ts_in_delta: int64
sequence: int64
ts_recv_ns: int64
ts_event_ns: int64
source_file: string
stage_version: string
```

输出 2: `schema=mbp10`

在上面公共列基础上增加:

```text
bid_px_00..bid_px_09: float64
ask_px_00..ask_px_09: float64
bid_sz_00..bid_sz_09: int64
ask_sz_00..ask_sz_09: int64
bid_ct_00..bid_ct_09: int64
ask_ct_00..ask_ct_09: int64
```

#### 并行边界

- Stage 2/3/4/5 以后都只消费 parquet，不再直接消费 DBN。
- 原始读取逻辑由 Stage 1 团队独占，后续团队不需要 Databento SDK 也能开发。

---

### Stage 2: Symbology Enrich

#### 目标

给 `und` 和 `opt` 加上统一的标的映射与合约元信息，形成可 join 的符号层。

#### 输入

- Stage 1 `trades`
- Stage 1 `mbp10`
- `definition_cache` 或 `definition_filtered`

#### 处理方法

1. 对 `und`:
   - 解析 `symbol=GCV6` 得到 `underlying_symbol=GC`, `contract_month=2026-10` 一类字段。
2. 对 `opt`:
   - 解析 `symbol=OGV6 P5155` 得到:
     - `underlying_symbol=GCV6`
     - `option_type=P`
     - `strike=5155.0`
3. 按 `trade_date + instrument_id + publisher_id` 做 definition join。
4. 生成统一键 `contract_key`。

#### 输出格式

输出文件: `schema=symbology`

```text
trade_date: date
asset_type: string
symbol: string
symbol_root: string
publisher_id: int32
instrument_id: int64
contract_key: string
underlying_symbol: string
underlying_root: string
contract_type: string      # FUT | OPT
option_type: string | null # C | P
strike: float64 | null
expiry_date: date | null
multiplier: float64 | null
stage_version: string
```

#### 并行边界

- 从 Stage 2 开始，下游禁止自行解析 symbol 字符串。
- 任何合约属性以 `symbology` 表为准。

---

### Stage 3: Sessionize And Order

#### 目标

统一排序、统一 session 标签、统一分桶，给后续 trades 清洗和盘口清洗提供稳定输入。

#### 输入

- Stage 1 `trades` / `mbp10`
- Stage 2 `symbology`

#### 处理方法

1. 按 `trade_date, asset_type, symbol, ts_recv_ns, sequence` 排序。
2. 补充 session 字段:
   - `session_name`: `PRE`, `RTH`, `POST`, `OFF`
   - `is_rth`: bool
3. 计算基础延迟字段:
   - `recv_event_latency_ns = ts_recv_ns - ts_event_ns`
4. 生成下游对齐需要的主键:
   - `record_id = hash(symbol, ts_recv_ns, sequence, action)`

#### 输出格式

输出文件: `schema=sessionized_trades` / `schema=sessionized_mbp10`

新增列:

```text
contract_key: string
underlying_symbol: string
session_name: string
is_rth: bool
recv_event_latency_ns: int64
record_id: string
```

#### 并行边界

- 下游统一使用 `record_id` 做排查和去重，不再自己拼主键。

---

### Stage 4: Trades Clean

#### 目标

对成交数据做可解释的清洗，但不物理删除重要异常，只打标签。

#### 输入

- Stage 3 `sessionized_trades`

#### 处理方法

1. 过滤硬错误:
   - `price <= 0`
   - `size <= 0`
   - 缺失 `instrument_id`
2. 保留软异常并打标签:
   - `side == N`
   - `recv_event_latency_ns < 0`
   - `flags != 0`
3. 标准化方向字段:
   - `trade_side = B | A | N`
4. 标准化成交级别字段:
   - `notional = price * size`
   - `is_valid_trade`
   - `quality_bucket`

#### 输出格式

输出文件: `schema=clean_trades`

新增列:

```text
trade_side: string
notional: float64
is_valid_trade: bool
quality_bucket: string     # clean | review | drop
drop_reason: string | null
has_flag_warning: bool
```

示例记录:

```text
trade_date=2026-05-27
asset_type=und
symbol=GCV6
trade_side=N
price=4586.0
size=1
notional=4586.0
quality_bucket=review
```

#### 并行边界

- Stage 4 只负责 trade 质量，不负责与 BBO 对齐。
- 任何 alpha 特征、买卖方向回补，都不应该在 Stage 4 实现。

---

### Stage 5: MBP10 Clean

#### 目标

把 10 档盘口清成稳定可消费的 book 快照流。

#### 输入

- Stage 3 `sessionized_mbp10`

#### 处理方法

1. 校验 top-of-book:
   - `bid_px_00 <= ask_px_00`
2. 校验 size/count 非负。
3. 打 book 质量标签:
   - `is_crossed_book`
   - `is_empty_book`
   - `is_sparse_book`
4. 提取统一的 top-of-book 字段:
   - `best_bid_px`
   - `best_ask_px`
   - `best_bid_sz`
   - `best_ask_sz`
   - `spread`
   - `mid_px`

#### 输出格式

输出文件: `schema=clean_mbp10`

新增列:

```text
best_bid_px: float64
best_ask_px: float64
best_bid_sz: int64
best_ask_sz: int64
spread: float64
mid_px: float64
is_crossed_book: bool
is_empty_book: bool
is_sparse_book: bool
book_quality_bucket: string    # clean | review | drop
```

示例记录:

```text
symbol=OGV6 P5155
best_bid_px=485.3
best_ask_px=769.6
best_bid_sz=2
best_ask_sz=2
spread=284.3
mid_px=627.45
book_quality_bucket=clean
```

#### 并行边界

- Stage 5 只产出“清洗后的盘口快照”，不做成交 asof join。
- Stage 6 团队只需要消费 top-of-book 字段，不必感知完整 10 档清洗细节。

---

### Stage 6: Trade Book Align

#### 目标

把清洗后的 trade 和 clean book 做时间对齐，产出研究和接口层最常用的统一底表。

#### 输入

- Stage 4 `clean_trades`
- Stage 5 `clean_mbp10`

#### 处理方法

1. 按 `symbol + ts_recv_ns` 做 backward asof join。
2. 控制容忍窗口，例如 `join_tolerance_ns=1_000_000_000`。
3. 生成对齐后衍生字段:
   - `book_age_ns`
   - `trade_vs_mid_bp`
   - `trade_vs_bid_ask_bucket`
4. 标记对齐成功与否:
   - `has_aligned_book`

#### 输出格式

输出文件: `schema=aligned_trade_book`

建议列:

```text
trade_date: date
asset_type: string
contract_key: string
symbol: string
underlying_symbol: string
ts_recv_ns: int64
ts_event_ns: int64
sequence: int64
price: float64
size: int64
trade_side: string
best_bid_px: float64 | null
best_ask_px: float64 | null
best_bid_sz: int64 | null
best_ask_sz: int64 | null
mid_px: float64 | null
spread: float64 | null
book_ts_recv_ns: int64 | null
book_age_ns: int64 | null
has_aligned_book: bool
trade_vs_mid_bp: float64 | null
trade_vs_bid_ask_bucket: string | null
quality_bucket: string
stage_version: string
```

#### 并行边界

- 策略、回测、特征工程团队直接消费 Stage 6，不再直接读 trades 或 mbp10。
- Stage 6 是研究层和服务层最重要的共享底表。

---

### Stage 7: QC And Publish

#### 目标

发布质量统计和最终 stage 发布状态，让上下游知道某天某合约是否可用。

#### 输入

- Stage 4 `clean_trades`
- Stage 5 `clean_mbp10`
- Stage 6 `aligned_trade_book`

#### 处理方法

1. 聚合统计:
   - trades 数量
   - clean/review/drop 比例
   - crossed book 比例
   - join miss 比例
2. 对每个 `trade_date + symbol` 输出可发布状态:
   - `publishable`
3. 发布统一 `release_manifest`。

#### 输出格式

输出 1: `schema=qc_summary`

```text
trade_date: date
asset_type: string
symbol: string
total_trades: int64
clean_trade_ratio: float64
review_trade_ratio: float64
drop_trade_ratio: float64
crossed_book_ratio: float64
aligned_success_ratio: float64
publishable: bool
stage_version: string
```

输出 2: `schema=release_manifest`

```text
trade_date: date
asset_type: string
symbol: string
input_stage_version: string
output_stage_version: string
publishable: bool
consumer_ready: bool
generated_at_ns: int64
```

#### 并行边界

- 服务端接口只读 `release_manifest` 里 `consumer_ready=true` 的分区。

---

## 4. 推荐的统一文件命名与目录契约

推荐统一目录:

```text
data/processed/fut_pipeline/
├── stage0_file_manifest/
├── stage1_normalized/
├── stage2_symbology/
├── stage3_sessionized/
├── stage4_clean_trades/
├── stage5_clean_mbp10/
├── stage6_aligned_trade_book/
└── stage7_qc_publish/
```

每个分区必须包含:

- `part-0000.parquet`
- `manifest.json`
- `_SUCCESS`

`manifest.json` 建议格式:

```json
{
  "stage_name": "stage4_clean_trades",
  "stage_version": "v1",
  "trade_date": "2026-05-27",
  "asset_type": "und",
  "schema": "clean_trades",
  "symbol_root": "GCV6",
  "row_count": 123456,
  "columns": ["trade_date", "asset_type", "symbol", "price"],
  "primary_keys": ["symbol", "ts_recv_ns", "sequence"],
  "upstream_version": "stage3_v1",
  "generated_at_ns": 1780000000000000000
}
```

---

## 5. 接口开发建议

如果目标是“阶段之间并行开发”，建议按下面方式分工:

1. A 组负责 Stage 0-1: 原始 DBN 扫描与标准化。
2. B 组负责 Stage 2-3: 元数据、symbology、排序和 session 化。
3. C 组负责 Stage 4: trades 清洗。
4. D 组负责 Stage 5: book 清洗。
5. E 组负责 Stage 6: trade-book 对齐。
6. F 组负责 Stage 7: QC 与发布。

每组只需要依赖前一组产出的 parquet + manifest，就可以独立 mock 开发。

最小 mock 数据契约:

- Stage 4 团队只需要一份 `sessionized_trades` parquet 样例。
- Stage 5 团队只需要一份 `sessionized_mbp10` parquet 样例。
- Stage 6 团队只需要一份 `clean_trades` 和一份 `clean_mbp10` 样例。
- 接口联调时以 `manifest.json.columns` 和 `primary_keys` 为准。

---

## 6. 最终建议

如果你要做服务接口，推荐直接对外暴露 Stage 6 和 Stage 7，不要直接暴露 Stage 1 原始标准化表。

推荐 API 层消费关系:

- 查询原始清洗后成交: Stage 4
- 查询原始清洗后盘口: Stage 5
- 查询对齐后研究底表: Stage 6
- 查询数据可用性与质量状态: Stage 7

这样可以保证:

- 上游清洗逻辑可替换
- 中间阶段可独立重跑
- 下游接口字段稳定
- 多团队可以围绕固定 contract 并行开发