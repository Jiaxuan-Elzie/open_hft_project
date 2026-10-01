# 数据清洗方案 — Polygon Massive(US Equity + Options)

先做个关键判断,然后给完整方案。

---

## 0. 关键判断: SIP per-exchange feed

- **股票 quotes**:Polygon 的 **CQS/UQDF SIP feed**(每个交易所每条报价),不是 NBBO
- **期权 quotes**:**OPRA full feed**(463 GB / 10 天 ≈ 46 GB/天,完全符合 full feed 量级)

**含义**:你需要自己重建 NBBO。这是 SOV 研究的硬性前置步骤。

另外注意几个数据健康问题:
- **股票 quotes 的 p95 gap 是 4.9 秒,max 是 8492 秒(2.4 小时)**:意味着原始数据里混有盘前盘后甚至跨日;采样跨越了非交易时段
- **期权 trades 只有 328 MB / 10 天**:期权成交极稀疏(这是事实,不是缺数据),做 SOV 时 trade-driven 信号会很稀
- **期权 quotes 的 median gap 2.9 ms**:活跃合约的报价更新很密,符合预期

---

## 1. 总体清洗 pipeline(7 个阶段)

```
Stage 0: 文件完整性检查
   ↓
Stage 1: 格式转换 (csv.gz → Parquet, 分区存储)
   ↓
Stage 2: 时间&会话过滤 (RTH / sip_timestamp 排序)
   ↓
Stage 3: Trade 清洗 (conditions, correction, outlier)
   ↓
Stage 4: Quote 清洗 (crossed/locked, zero, stale)
   ↓
Stage 5: NBBO 重建 (per-exchange → BBO)
   ↓
Stage 6: 期权符号规范化 + 与标的对齐
   ↓
Stage 7: 验证 & QC 报告
```

---

## Stage 0: 文件完整性检查

**问题**:你只有 10 天 quotes 但 28 天 trades(股票),期权 quotes/trades 都是 10 天但只覆盖 5 月,这种 **不对称会导致研究日期对齐失败**。

**操作**:
1. 列出每类数据的实际日期 set,取交集作为 **可用研究日期**
2. 对每个文件做 `gzip -t` 测试完整性
3. 抽样读首尾几行,确认没有截断
4. 输出一张表:`(date, und_quote, und_trade, opt_quote, opt_trade)` 全部为 ✓ 才进入下一阶段

```python
# 关键:研究只能在四类全齐的日期上做
valid_dates = und_q_dates & und_t_dates & opt_q_dates & opt_t_dates
```

---

## Stage 1: 格式转换 csv.gz → Parquet

**为什么**:csv.gz 不能列剪枝、不能谓词下推。期权 quotes 463 GB,如果不转 parquet,后续每次跑都要全量扫,实验迭代会崩溃。

**分区方案**:

```
parquet_root/
├── und_quotes/date=2026-05-01/ticker=SPY/part-0.parquet
├── und_trades/date=2026-05-01/ticker=SPY/part-0.parquet
├── opt_quotes/date=2026-05-01/underlying=SPY/part-0.parquet
└── opt_trades/date=2026-05-01/underlying=SPY/part-0.parquet
```

为什么按 `underlying` 分区而不是 `ticker`(option contract):S&P 500 一天约 50 万个活跃合约,每个合约一个文件会引发 small-file 灾难。按 underlying 分区(500 个),每个文件几百 MB,刚好。

**dtype 规约(关键,影响磁盘和速度)**:

| 字段 | 原 | 目标 |
|---|---|---|
| `sip_timestamp` | int64 ns | int64 ns(保留) |
| `bid_price/ask_price/price` | float64 | float64(期权小数多,不要 float32) |
| `bid_size/ask_size/size` | int64 | int32 |
| `bid_exchange/ask_exchange/exchange` | int | int8 |
| `conditions` | str/list | int16 array |
| `ticker` | str | category |
| `sequence_number` | int64 | int64 |

**期望压缩比**:csv.gz → parquet (zstd) 通常再压 30-50%,463 GB 期权 quotes 转完应该在 **180-250 GB**。

---

## Stage 2: 时间 & 会话过滤

**核心字段选择**:用 `sip_timestamp` 作为主时间,因为:
- `participant_timestamp` 是交易所发出时间,各交易所时钟不一定同步
- `trf_timestamp` 只有 TRF(暗池)成交才有
- `sip_timestamp` 是 SIP 接收时间,**全市场统一参考系**,符合 NBBO 监管定义

**过滤规则**:

```python
# 只保留常规时段 (Regular Trading Hours, ET)
RTH_START = "09:30:00.000000000"
RTH_END   = "16:00:00.000000000"

# 注意 sip_timestamp 是 UTC 纳秒,要先转 America/New_York
# 处理 DST 切换(3 月和 11 月)
```

**关于盘前盘后**:
- 如果做 SOV,**只保留 RTH**(因为期权基本只在 RTH 有流动性)
- 如果做事件研究、隔夜信息,可以保留盘前盘后但单独打 `session` 标签

**排序**:每个 (date, ticker) 内按 `(sip_timestamp, sequence_number)` 升序排序,这是后续所有 join 和 NBBO 重建的前提。

---

## Stage 3: Trade 清洗

### 3.1 股票 Trades

**(a)Correction 过滤**

```python
# correction == 0 才是有效成交,其他是更正/取消
trades = trades[trades['correction'] == 0]
```

**(b)Conditions 过滤(Polygon 用 SIP condition codes)**

研究 SOV 时的标准做法是只保留对价格发现有贡献的成交:

| 处理 | Conditions(常见) |
|---|---|
| **保留** | Regular trade, Intermarket Sweep |
| **剔除** | Odd lot (条件码 37)、Cash sale、Next day、Sold last、Out of sequence、Average price trade、Form T(盘前盘后)、Derivative priced |
| **可选剔除** | TRF 成交(暗池),如果只研究 lit market |

具体 condition code 对照表参考 Polygon 文档的 `conditions` mapping(SIP codes for Tape A/B/C 不一样)。

**(c)价格异常**

```python
# 与同合约前 N 笔的中位价比较
# |price - rolling_median| / rolling_median > 0.10 标为异常
# 不要直接删,打 outlier flag,后续按需过滤
```

**(d)零价/零量**

```python
trades = trades[(trades['price'] > 0) & (trades['size'] > 0)]
```

### 3.2 期权 Trades

期权 trades 字段没有 `correction`(你的 schema 里没有该字段—等等,实际上有,在股票里有,期权里没列出,确认一下),也没有 sequence_number,处理更简单:

- 剔除 `price <= 0` 或 `size <= 0`
- conditions 过滤期权特有码:剔除 spread trades (multi-leg) 中的非主腿、剔除 cancellation
- 期权价格上下界检查:`price >= 0.01`(最小 tick),`price <= max(strike, underlying_price * 2)` 防止脏数据

---

## Stage 4: Quote 清洗

### 4.1 共同规则

**(a)零价/零量**:`bid_price > 0 AND ask_price > 0 AND bid_size > 0 AND ask_size > 0`

**(b)Crossed/Locked 在单交易所内的报价**

```python
# 单个交易所内:bid >= ask 是异常,标记并剔除
single_ex_crossed = quotes[quotes['bid_price'] >= quotes['ask_price']]
```

注意:**跨交易所** 的 crossed/locked NBBO 是合法存在的,要在 NBBO 重建后再处理。

**(c)Spread 异常宽**

```python
# 股票:spread / mid > 0.05 通常是脏数据或停牌
# 期权:更宽松,因为深度 OTM 本来就宽,可以用 spread > strike * 0.5 这种
```

**(d)Conditions/Indicators 过滤**

| 剔除类型 | 含义 |
|---|---|
| Trading halt | 停牌期间 quotes 不可信 |
| Quote indicator = "Closing" / "Opening" | 开收盘特殊 quote |
| Non-firm quote / Indicative | 非可执行报价 |

### 4.2 期权 quotes 特别处理

**Stale quote 检测**:

```python
# 如果同一合约 quote 不更新超过 N 秒,
# 后续基于它做 sign 时打 stale flag
quotes['gap_to_prev'] = quotes['sip_timestamp'].diff()
quotes['stale'] = quotes['gap_to_prev'] > 300_000_000_000  # 5 min
```

期权 quotes 463 GB,清洗时**必须 streaming 处理**,按 (date, underlying) 一块一块来,不要尝试全部加载。

---

## Stage 5: NBBO 重建(核心步骤)

这是你研究的关键基础设施。算法本身不复杂,但工程量大。

### 5.1 算法

每个 (option_contract or stock_ticker) 维护一个 **per-exchange book**:

```python
# 状态:每个交易所最新的 (bid, bid_size, ask, ask_size)
state = {ex_id: (bid, bsz, ask, asz)}

# 事件流:按 sip_timestamp 排序的 quote 更新
for q in quote_stream:
    state[q.bid_exchange] = update_bid(...)
    state[q.ask_exchange] = update_ask(...)

    nbb = max(s.bid for s in state.values() if s.bid > 0)
    nbo = min(s.ask for s in state.values() if s.ask > 0)

    if (nbb, nbo) changed:
        emit NBBO event
```

### 5.2 关键工程细节

**(1)每条 quote 同时更新 bid 和 ask**?

Polygon SIP quote 一条记录可能 `bid_exchange != ask_exchange`,这种情况罕见但要注意。一般同一条 quote 来自同一交易所,即 `bid_exchange == ask_exchange`,所以单交易所同时更新 bid 和 ask。如果你看到不一致,**以 quote 中各自的 exchange 字段为准**,分别更新。

**(2)Cross-exchange crossed NBBO 处理**

NBBO 出现 NBB > NBO 时:
- 多数情况持续几毫秒(latency arbitrage 窗口)
- 不要剔除,**打 `crossed` flag**,做 SOV 时单独处理

**(3)增量 emit**

不要每条 quote 都输出 NBBO,**只在 NBBO 实际变化时输出**(这就是 OPRA NBBO feed 的逻辑)。这一步能把数据量从全量 quotes 砍掉 5-10x。

**(4)分块处理**

每个 (date, underlying) 独立处理。期权一个 underlying 一天的 quotes 大约 100MB-1GB,可以全部进内存处理。

### 5.3 输出 schema

```
nbbo/
├── date / underlying / contract /
    columns: [sip_timestamp, nbb, nbb_size, nbb_exchange,
              nbo, nbo_size, nbo_exchange, crossed_flag, locked_flag]
```

**预期数据量**:期权 NBBO 大约是 full quotes 的 20-30%(60-90 GB,zstd parquet)。

---

## Stage 6: 期权符号规范化 + 与标的对齐

### 6.1 OCC 符号解析

Polygon 的期权 ticker 格式:`O:SPY240119C00500000`

```python
# 解析为结构化字段
parse_occ('O:SPY240119C00500000') = {
    'underlying': 'SPY',
    'expiry': date(2024, 1, 19),
    'cp': 'C',
    'strike': 500.0,
}
```

**关键**:把这些字段作为列存进 parquet,后续筛选(选 ATM、近月)就不用 string parsing。

### 6.2 标的价格对齐(SOV 必需)

对每个期权 trade/quote,需要找到对应时刻的 underlying mid price。

```python
# asof join: 每个 option event ← 最近的 underlying NBBO mid
opt_with_und = asof_join(
    left=opt_events,
    right=und_nbbo,
    on='sip_timestamp',
    by='underlying',
    direction='backward',
    tolerance='1s'  # 超过 1 秒标 unreliable
)
```

注意:**先重建 underlying NBBO,再做这一步**。

### 6.3 隐含波动率 / Greeks(可选,但 SOV 常用)

如果研究需要,在这一步预计算:
- moneyness = K / S 或 log(K/S) / (σ√τ)
- 时间到期 τ
- IV(用 BS 模型反推,需要无风险利率和股息率)
- delta, gamma, vega

预算这些计算量,放在 cleaning 最后一步,作为可选 feature 表。

---

## Stage 7: 验证 & QC 报告

每天清洗完,自动生成一份 QC 报告。这是你**唯一能发现脏数据的机会**,不要省。

**强制检查项**:

| 检查 | 阈值/规则 |
|---|---|
| Trade count by ticker | 与历史 ±50% 内 |
| NBBO update count | 与历史 ±50% 内 |
| Crossed NBBO 占比 | < 1% |
| Locked NBBO 占比 | < 5% |
| RTH 内 quote gap > 1 秒 | 占比记录(冷门合约会高) |
| Trade 价格在 NBBO 内占比 | 应 > 95%(at-or-better rule) |
| 时间戳乱序 | sequence_number 递增 |
| 重复 (sip_ts, seq) | 应为 0 |

**SOV 专属检查**:

| 检查 | 用途 |
|---|---|
| Quote rule 可分类比例 | 应 > 60% |
| Tick rule fallback 比例 | 监控 mid trade 占比 |
| Stale NBBO trade 比例 | 应 < 10%,否则信号不可信 |

---

## 关于工程实现的几点建议

**(1)用 Polars 或 DuckDB,不要用 Pandas**

463 GB 期权 quotes,Pandas 会让你想哭。**Polars 的 streaming engine** 或 **DuckDB 的 out-of-core** 都能处理。我个人推荐 Polars,API 干净,速度比 Pandas 快 10-50x。

**(2)NBBO 重建用纯 Python + numpy/numba 写**

Polars/DuckDB 不擅长这种状态机逻辑。NBBO 重建本质是 event loop,用 numba JIT 一个 per-(date, underlying) 的 worker,然后用 multiprocessing 并行,**单机 16 核处理 10 天期权数据应该 4-8 小时内**。

**(3)分阶段保存中间结果**

每个 stage 输出独立 parquet,出错可以重跑单一阶段。**不要写一个 end-to-end 长 pipeline**。

**(4)磁盘预算**

```
原始:        492 GB
Parquet:     ~250 GB(stage 1)
清洗后:      ~230 GB(stage 4)
NBBO:        ~80 GB(stage 5)
对齐+特征:   ~120 GB(stage 6)
─────────────
总占用:      ~700 GB(包含原始)
```

如果磁盘紧张,Stage 1 完成后可以考虑把原始 csv.gz 移到冷存储或删掉(确保 parquet 没问题)。

---

## 推荐执行顺序(给你两周时间预算)

| 周 | 任务 |
|---|---|
| **Day 1-2** | Stage 0 + 1:文件验证 + csv.gz → parquet。先跑 1 天小样本调通,再全量 |
| **Day 3-4** | Stage 2 + 3:时间过滤 + trade 清洗,生成第一版 clean trades |
| **Day 5-7** | Stage 4 + 5:**最重要、最耗时**。quote 清洗 + NBBO 重建 |
| **Day 8-9** | Stage 6:期权符号 + asof join + 可选 IV |
| **Day 10** | Stage 7:QC 报告 + 抽样人工检查 |
| **Day 11-14** | 跑 SOV pilot,根据结果反向调整 cleaning 参数 |

---

要不要我先写出 **Stage 1(csv.gz → 分区 parquet,Polars streaming 版)** 的完整代码?这是后面所有步骤的基础,跑通了你就有干净的工作底座了。