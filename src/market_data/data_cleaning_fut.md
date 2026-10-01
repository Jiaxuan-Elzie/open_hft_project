# 数据清洗方案 — Databento Trades + MBP-10

> 基于此前讨论整理的最终执行版方案。原则:**Databento 数据本身已较干净,清洗重点不是"过滤脏数据",而是"建立正确的对齐与一致性"**。四个关键对齐:instrument_id → raw_symbol、ts_recv 时间轴、book levels 一致性、trade-BBO 精确对齐。

---

## 一、核心技术决策(写代码前先定死)

| 决策项 | 选择 | 理由 |
|---|---|---|
| 主时间戳 | **ts_recv** | 单一参考系,跨 venue 可对齐 |
| 价格类型 | **全程 int64 纳元** | 仅在最终输出做一次 `/ 1e9` |
| Trade side 来源 | **优先 Databento 原生 side**,'N' 时 fallback quote rule | 原生 side 基于 venue 信息,远准于 Lee-Ready |
| BBO 来源 | **直接取 MBP-10 levels[0]**,不自己 replay | 已是更新后快照,replay 仅用于一致性校验 |
| 数据栈 | **Databento SDK + Polars (lazy/streaming)** | DBN→Arrow→Polars 零拷贝 |
| Parquet 转换 | **可选**,仅在用 DuckDB/Spark 或多人协作时做 | DBN 已紧凑,转换体积比≈1:1 |
| Symbology join | **必须按 (date, instrument_id, publisher_id) 三键 join** | instrument_id 跨日复用 |

---

## 二、Pipeline 总体结构

清洗分 8 个阶段,每个阶段独立持久化,支持断点续跑与单阶段重跑。

```
Stage 0  文件 & schema 完整性检查
Stage 1  DBN → Parquet 转换(可选)
Stage 2  Symbology 解析(关键前置)
Stage 3  时间戳标准化 + 会话过滤
Stage 4  Trades 清洗
Stage 5  MBP-10 清洗 + book 一致性校验
Stage 6  BBO/NBBO 派生 + Trade-Quote 对齐
Stage 7  QC 报告 + 抽样人工核验
```

目录约定:

```
data_root/
├── stage0_validated/
├── stage1_parquet/        (可选)
├── stage2_symbol_map/
├── stage3_filtered/
├── stage4_trades_clean/
├── stage5_mbp10_clean/
├── stage6_aligned/        ← 研究层底表
└── stage7_qc_reports/
```

每个 stage 输出带版本号目录(如 `v20260601_run01/`),不覆盖历史结果。

---

## 三、各阶段执行细则

### Stage 0:文件 & Schema 完整性检查

**目标**:在进入清洗前识别所有不可用文件,避免下游静默错误。

**操作**:
1. 用 `DBNStore.from_file().metadata` 检查 record_count、start_ts、end_ts、schema 与请求范围匹配
2. 流式遍历到 EOF 捕获异常 → 截断/损坏文件入重下载队列
3. Schema 一致性:trades 文件必须 schema=`trades`,MBP-10 必须 schema=`mbp-10`
4. 输出可用日期表 `(date, symbol, trades_ok, mbp10_ok, definition_ok)`,**取四列全 ok 的交集作为正式研究日期**

### Stage 1:DBN → Parquet(可选)

**何时做**:下游使用 DuckDB / Polars 大批量查询、或需要 (date, symbol) 双层分区裁剪。
**何时跳过**:单机 Python + Databento SDK 直读完全够用。

**dtype 规约(强制)**:

| 字段类别 | dtype |
|---|---|
| ts_recv / ts_event | int64 (ns) |
| ts_in_delta | int32 |
| instrument_id | uint32 |
| publisher_id | uint16 |
| price / bid_px_NN / ask_px_NN | **int64,禁止早转 float** |
| size / bid_sz_NN / ask_sz_NN / bid_ct_NN / ask_ct_NN | uint32 |
| action / side | category |
| flags | uint8 bitfield(**不拆分成多 bool 列**) |
| sequence | uint32 |
| raw_symbol(映射后) | category |

分区:`schema/date=YYYY-MM-DD/symbol=XXX/part-N.parquet`,zstd 压缩。

### Stage 2:Symbology 解析

**关键前置,跑不通整条 pipeline 不能继续。**

```python
sym_map = client.symbology.resolve(
    dataset='XNAS.ITCH',
    symbols=[...],
    stype_in='raw_symbol',
    stype_out='instrument_id',
    start_date=...,
    end_date=...,
)
# 展开为长表 (date, instrument_id, raw_symbol, publisher_id)
```

Join 规则:

```python
named = trades.join(
    sym_map,
    on=['date', 'instrument_id', 'publisher_id'],
    how='left'
)
assert named['raw_symbol'].null_count() == 0   # 任何 null 必须排查
```

期权场景:同时拉 `definition` schema,从中解析 OCC 字段(strike / expiry / cp_flag),不自写 OSI parser。

### Stage 3:时间戳标准化 + 会话过滤

1. 统一以 **ts_recv** 排序:`sort(['instrument_id', 'ts_recv', 'sequence'])`
2. UTC 纳秒 → America/New_York,**注意 DST 与半天交易日**
3. 打 session 标签(PRE / RTH / POST / AUCTION_OPEN / AUCTION_CLOSE),**不要直接物理删除非 RTH 数据**

```python
RTH_START_NY = "09:30:00"
RTH_END_NY   = "16:00:00"
```

### Stage 4:Trades 清洗

按顺序执行 5 步:

```python
F_BAD_TS_RECV = 1 << 4

trades_clean = (
    trades
    .filter(pl.col('action') == 'T')                            # 1) 仅 trade,剔 'F'
    .filter((pl.col('flags') & F_BAD_TS_RECV) == 0)             # 2) 剔 ts_recv 不可信
    .filter((pl.col('price') > 0) & (pl.col('size') > 0))       # 3) 剔非法价量
    .with_columns(                                              # 4) side 状态打标
        side_known=pl.col('side').is_in(['B', 'A'])
    )
    # 5) auction trades 打 flag,做 SOV 时下游剔除
)
```

**重要**:outlier 与 auction 都打 flag,**不删**,留给下游按需过滤。

### Stage 5:MBP-10 清洗 + Book 一致性校验

**清洗规则**:

```python
# 1) 剔 ts_recv 不可信
mbp10 = mbp10.filter((pl.col('flags') & F_BAD_TS_RECV) == 0)

# 2) 单 venue 内 crossed 检测(正常应 < 0.01%)
crossed = mbp10.filter(
    (pl.col('bid_px_00') > 0) &
    (pl.col('ask_px_00') > 0) &
    (pl.col('bid_px_00') >= pl.col('ask_px_00'))
)

# 3) Levels monotonicity:bid 递减、ask 递增,违反 > 0.1% → 联系 Databento support

# 4) Snapshot / clear 保留(用于 book 重建),但不计入"事件统计"
#    action == 'R' 或 (flags & F_SNAPSHOT) != 0
```

**性能要点**:活跃股票单日 MBP-10 可达千万级,必须用 lazy + streaming:

```python
(pl.scan_parquet('mbp10/date=*/...')
   .filter(...)
   .sort(['instrument_id', 'ts_recv', 'sequence'])
   .sink_parquet(out_path))
```

### Stage 6:BBO 派生 + Trade-Quote 对齐(研究层底表)

**单 venue BBO**:直接取 levels[0]。若只需 top-of-book 变化点,用 `flags & F_TOB` 过滤,数据量降 5–10 倍;或干脆订阅 `tbbo` schema。

**跨 venue NBBO**(若需):每 publisher 一个 BBO,事件流按 ts_recv 排序,跨 publisher 取 `max(bid)` / `min(ask)`,**仅在 NBBO 实际变化时 emit**;瞬时 crossed 打 flag 不剔除。状态机用 Numba JIT,**绝不在 Python 解释器跑事件循环**。

**Trade-Quote 对齐**(asof backward):

```python
aligned = trades.join_asof(
    bbo,
    on='ts_recv',
    by=['instrument_id', 'publisher_id'],
    strategy='backward',
    tolerance='1s'                  # join 不到的标 unreliable
).with_columns(
    mid    = (pl.col('bid') + pl.col('ask')) / 2,
    spread =  pl.col('ask') - pl.col('bid'),
    db_sign = pl.when(pl.col('side') == 'B').then(1)
                .when(pl.col('side') == 'A').then(-1)
                .otherwise(0),
    qr_sign = pl.when(pl.col('price') > pl.col('mid')).then(1)
                .when(pl.col('price') < pl.col('mid')).then(-1)
                .otherwise(0),
    final_sign = pl.when(pl.col('side') != 'N').then('db_sign')
                   .otherwise('qr_sign'),    # 原生 side 优先
)
```

**最后一步**(整条 pipeline 唯一一次 float 转换):

```python
aligned = aligned.with_columns(
    price_f = pl.col('price') / 1e9,
    bid_f   = pl.col('bid')   / 1e9,
    ask_f   = pl.col('ask')   / 1e9,
)
```

### Stage 7:QC 报告 + 人工核验

**自动化检查阈值**:

| 类别 | 指标 | 正常范围 |
|---|---|---|
| Trades | side == 'N' 占比 (RTH) | < 10% |
| Trades | F_BAD_TS_RECV 占比 | < 0.1% |
| Trades | trade @ or within BBO 占比 | > 95% |
| MBP-10 | 单 venue crossed 占比 | < 0.01% |
| MBP-10 | levels monotonicity 违反 | < 0.1% |
| MBP-10 | ts_in_delta p99 | < 1ms |
| Symbology | instrument_id → raw_symbol 映射率 | 100% |
| SOV | quote rule 与 db_sign 一致率 | > 90% |
| SOV | stale BBO trade(BBO age > 100ms)占比 | < 5% |

**人工核验**:每天挑 1–2 只活跃股票画 BBO + trades 时序图,肉眼检查 trade 是否跑出 BBO、BBO 是否反复跳动。**自动化不可替代。**

---

## 四、工程化实现要点

**并行化**。Stage 1–5 按 (date, symbol) 二维并行,multiprocessing 即可。Stage 6 必须等 Stage 5 完成。单机 16 核处理 10 天 × 50 symbol 的全量约 4–8 小时。

**幂等性**。每 stage 输出带版本目录;开始前检查目标是否已存在,有则跳过(除非 `--force`)。

**磁盘预算**(参考:10 天 × 50 活跃股票):
- 原始 DBN(zstd):trades ~5–10 GB,MBP-10 ~150–300 GB
- Parquet 转换:体积比 ≈ 1:1
- Stage 6 输出(trade + BBO 对齐宽表):trades 体积 × 5–10
- **总磁盘按原始数据 2 倍预留**

**Book replay 不要自己写**。需要 levels 0–9 之外的 book 时,用 Databento 官方 book builder 或参考 databento-rs。Auction / self-trade / reroute 等 corner case 自写必有 bug。

**Dataset 选错的代价**。`XNAS.ITCH` ≠ `DBEQ.BASIC` ≠ `EQUS.MINI`,后续 NBBO 语义和清洗逻辑都不同;期权用 `OPRA.PILLAR`,分区策略要从 by symbol 改为 by underlying。

---

## 五、执行排期

| 时间 | 任务 |
|---|---|
| step1 | Stage 0 + Stage 2:**先把 symbology join 跑通** |
| step2 | Stage 1(若做)+ Stage 3:1 天 + 少量 symbol 调通,再全量 |
| step3 | Stage 4:trades 清洗,验证 flags 过滤与 auction 识别 |
| step4 | Stage 5 + Stage 6:最重的部分,book 校验 + BBO 派生 + 对齐 |
| step5 | Stage 7:QC 报告 + 抽样画图人工核验 |
| step6 | SOV pilot + 反向调参(auction 规则 / stale BBO 阈值 / tolerance) |

---

## 六、与 Polygon SIP 方案的核心差异

| 维度 | Polygon SIP | Databento |
|---|---|---|
| 主清洗负担 | condition codes 过滤 + NBBO 重建 | symbology 解析 + book 一致性 |
| BBO 来源 | 自建(per-exchange replay) | MBP-10 自带 levels[0] |
| Trade side | 自推(quote rule / tick rule) | 原生 side 字段(更准) |
| 主时间戳 | sip_timestamp | ts_recv |
| 原始格式 | csv.gz,**必须**转 Parquet | DBN 二进制,转不转随意 |
| Condition codes | 复杂,需查表 | 已结构化为 action/side/flags |

**一句话总结**:Databento 清洗的成败取决于四个对齐(symbology / 时间轴 / book / trade-quote),把这四件事做对,SOV 与微观结构研究的底座就稳了。