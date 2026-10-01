# Trade Stats Summary

本文件整理了 FUT / STOCK 的 UND 与 OPT 在当前样本日上的 `trades`、`mbp-10` / `quotes` 更新统计，保留原始统计结果，但改成更适合阅读的 Markdown 结构。

## Snapshot

| Scope | Sample Day | Feed | File Count | Symbol Count | Total Rows |
| --- | --- | --- | ---: | ---: | ---: |
| FUT UND | 2026-05-27 | trades | 3 | 3 | - |
| FUT UND | 2026-05-27 | mbp-10 | 3 | 3 | - |
| FUT OPT | 2026-05-27 | trades | 35 | 652 | - |
| FUT OPT | 2026-05-27 | mbp-10 | 35 | 3985 | - |
| STOCK UND | 2026-05-14 | trades | 1 | 503 | 41,501,895 |
| STOCK UND | 2026-05-14 | quotes | 1 | 503 | 38,161,185 |
| STOCK OPT | 2026-05-14 | trades | 1 | 130,802 | 4,012,240 |
| STOCK OPT | 2026-05-14 | quotes | 1 | 533,375 | 2,297,747,750 |

## Coverage Overview

| Scope | Total Unique Symbols | Both Feeds Present | Feed A Only | Feed B Only |
| --- | ---: | ---: | ---: | ---: |
| FUT UND (`trades` / `mbp-10`) | 3 | 3 | 0 | 0 |
| FUT OPT (`trades` / `mbp-10`) | 3985 | 652 | 0 | 3333 |
| STOCK UND (`trades` / `quotes`) | 503 | 503 | 0 | 0 |
| STOCK OPT (`trades` / `quotes`) | 533411 | 130766 | 36 | 402609 |

## FUT UND

### Feed Summary

| Feed | File Count | Symbol Count | Mean | Median | P90 | P99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| trades | 3 | 3 | 41,651.00 | 17,128.00 | 89,502.40 | 105,786.64 | 107,596 |
| mbp-10 | 3 | 3 | 981,695.70 | 113,967.00 | 2,257,297.00 | 2,739,546.00 | 2,793,129 |

### trades

```text
describe:
count         3.000000
mean      41651.000000
std       57731.718483
min         229.000000
25%        8678.500000
50%       17128.000000
75%       62362.000000
90%       89502.400000
99%      105786.640000
max      107596.000000

update-count distribution:
trades_updates
(-0.001, 1.0]            0
(1.0, 10.0]              0
(10.0, 100.0]            0
(100.0, 1000.0]          1
(1000.0, 10000.0]        0
(10000.0, 100000.0]      1
(100000.0, 1000000.0]    1
(1000000.0, inf]         0
```

Top symbols:

```text
GCQ6    107596
GCM6     17128
GCV6       229
```

### mbp-10

```text
describe:
count    3.000000e+00
mean     9.816957e+05
std      1.569207e+06
min      3.799100e+04
25%      7.597900e+04
50%      1.139670e+05
75%      1.453548e+06
90%      2.257297e+06
99%      2.739546e+06
max      2.793129e+06

update-count distribution:
mbp-10_updates
(-0.001, 1.0]            0
(1.0, 10.0]              0
(10.0, 100.0]            0
(100.0, 1000.0]          0
(1000.0, 10000.0]        0
(10000.0, 100000.0]      1
(100000.0, 1000000.0]    1
(1000000.0, inf]         1
```

Top symbols:

```text
GCQ6    2793129
GCM6     113967
GCV6      37991
```

### Combined Coverage

```text
total unique symbols: 3
symbols with both trades and mbp-10: 3
symbols with trades only: 0
symbols with mbp-10 only: 0

combined per-symbol updates preview:
      trades_updates  mbp-10_updates
GCQ6          107596         2793129
GCM6           17128          113967
GCV6             229           37991
```

## FUT OPT

### Feed Summary

| Feed | File Count | Symbol Count | Mean | Median | P90 | P99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| trades | 35 | 652 | 5.81 | 3.00 | 13.90 | 45.96 | 88 |
| mbp-10 | 35 | 3985 | 88,796.32 | 58,146.00 | 242,424.80 | 436,813.92 | 577,376 |

### trades

```text
describe:
count    652.000000
mean       5.812883
std        9.677850
min        1.000000
25%        1.000000
50%        3.000000
75%        6.000000
90%       13.900000
99%       45.960000
max       88.000000

update-count distribution:
trades_updates
(-0.001, 1.0]          212
(1.0, 5.0]             262
(5.0, 10.0]             89
(10.0, 50.0]            84
(50.0, 100.0]            5
(100.0, 500.0]           0
(500.0, 1000.0]          0
(1000.0, 5000.0]         0
(5000.0, 10000.0]        0
(10000.0, 100000.0]      0
(100000.0, inf]          0
```

Top 20 symbols:

```text
OGN6 C5000    88
OGN6 P4000    87
OGN6 P4100    80
OGN6 C4550    76
OGN6 P4400    66
OGN6 C4700    48
OGN6 P4150    48
OGN6 P4330    44
OGN6 P4300    41
OGN6 C4600    38
OGN6 C4500    38
OGN6 C4800    37
OGQ6 C5000    37
OGQ6 C5250    37
OGN6 P4440    36
OGN6 C4750    36
OGN6 P4225    34
OGQ6 P4300    33
OGN6 C4525    32
OGN6 C4650    30
```

### mbp-10

```text
describe:
count      3985.000000
mean      88796.321957
std      102263.858474
min          14.000000
25%       14969.000000
50%       58146.000000
75%      118084.000000
90%      242424.800000
99%      436813.920000
max      577376.000000

update-count distribution:
mbp-10_updates
(-0.001, 1.0]             0
(1.0, 5.0]                0
(5.0, 10.0]               0
(10.0, 50.0]              1
(50.0, 100.0]             0
(100.0, 500.0]            0
(500.0, 1000.0]           3
(1000.0, 5000.0]        650
(5000.0, 10000.0]       226
(10000.0, 100000.0]    2012
(100000.0, inf]        1093
```

Top 20 symbols:

```text
OGN6 P4500    577376
OGN6 P4495    541634
OGN6 P4505    531278
OGN6 P4510    527260
OGN6 C4500    527097
OGN6 P4520    519142
OGN6 P4545    502783
OGN6 P4600    502253
OGN6 P4525    501572
OGN6 C4495    498265
OGN6 C4515    495933
OGN6 C4505    495867
OGN6 C4510    494747
OGN6 C4600    494161
OGN6 P4515    490375
OGN6 P4535    489579
OGN6 P4540    486389
OGN6 C4525    482691
OGN6 P4530    481530
OGN6 C4545    479832
```

### Combined Coverage

```text
total unique symbols: 3985
symbols with both trades and mbp-10: 652
symbols with trades only: 0
symbols with mbp-10 only: 3333

combined per-symbol updates preview:
            trades_updates  mbp-10_updates
OGN6 C5000              88           67907
OGN6 P4000              87           87354
OGN6 P4100              80          164516
OGN6 C4550              76          428488
OGN6 P4400              66          429817
OGN6 C4700              48          288789
OGN6 P4150              48          205364
OGN6 P4330              44          372744
OGN6 P4300              41          331739
OGN6 C4500              38          527097
OGN6 C4600              38          494161
OGN6 C4800              37          194905
OGQ6 C5000              37          157949
OGQ6 C5250              37           62336
OGN6 P4440              36          454641
OGN6 C4750              36          247465
OGN6 P4225              34          267348
OGQ6 P4300              33          305065
OGN6 C4525              32          482691
OGN6 C4650              30          387942
```

## STOCK UND

Sample day: `2026-05-14`

### Feed Summary

| Feed | File Path | Symbol Count | Total Rows | Mean | Median | P90 | P99 | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| trades | `data/raw_data/stock/underlyings/trades/2026/05/2026-05-14.csv.gz` | 503 | 41,501,895 | 82,508.74 | 44,972.00 | 126,000.60 | 711,915.20 | 2,782,341 |
| quotes | `data/raw_data/stock/underlyings/quotes/2026/05/2026-05-14.csv.gz` | 503 | 38,161,185 | 75,867.17 | 35,483.00 | 159,157.20 | 668,366.20 | 2,035,270 |

### trades

```text
describe:
count    5.030000e+02
mean     8.250874e+04
std      1.698799e+05
min      5.141000e+03
25%      3.111800e+04
50%      4.497200e+04
75%      7.511400e+04
90%      1.260006e+05
99%      7.119152e+05
max      2.782341e+06

update-count distribution:
trades_updates
(-0.001, 1.0]                0
(1.0, 10.0]                  0
(10.0, 100.0]                0
(100.0, 1000.0]              0
(1000.0, 10000.0]            2
(10000.0, 100000.0]        424
(100000.0, 1000000.0]       73
(1000000.0, 10000000.0]      4
(10000000.0, inf]            0
```

Top 20 symbols:

```text
NVDA     2782341
TSLA     1192874
INTC     1068393
MU       1019043
CSCO      756662
GOOGL     712836
AAPL      666797
MSFT      600430
PLTR      596863
SNDK      499140
AMZN      488493
AMD       484251
AVGO      462474
F         422910
QCOM      362489
NOW       357387
GOOG      351011
AMAT      349499
ORCL      332662
META      320536
```

### quotes

```text
describe:
count    5.030000e+02
mean     7.586717e+04
std      1.428877e+05
min      4.017000e+03
25%      2.020300e+04
50%      3.548300e+04
75%      7.641200e+04
90%      1.591572e+05
99%      6.683662e+05
max      2.035270e+06

update-count distribution:
quotes_updates
(-0.001, 1.0]                0
(1.0, 10.0]                  0
(10.0, 100.0]                0
(100.0, 1000.0]              0
(1000.0, 10000.0]           22
(10000.0, 100000.0]        389
(100000.0, 1000000.0]       90
(1000000.0, 10000000.0]      2
(10000000.0, inf]            0
```

Top 20 symbols:

```text
NVDA     2035270
INTC     1150864
AMZN      895026
SMCI      829656
CSCO      670596
GOOGL     668544
HPE       659652
TSLA      614044
AAPL      553524
GOOG      448546
MU        433336
PLTR      421026
HOOD      419622
OXY       367024
BAC       323220
NOW       322216
AMD       313353
NFLX      312674
F         271846
UBER      268166
```

### Combined Coverage

```text
total unique symbols: 503
symbols with both trades and quotes: 503
symbols with trades only: 0
symbols with quotes only: 0

combined per-symbol updates preview:
       trades_updates  quotes_updates
NVDA          2782341         2035270
TSLA          1192874          614044
INTC          1068393         1150864
MU            1019043          433336
CSCO           756662          670596
GOOGL          712836          668544
AAPL           666797          553524
MSFT           600430          257279
PLTR           596863          421026
SNDK           499140          191685
AMZN           488493          895026
AMD            484251          313353
AVGO           462474          226232
F              422910          271846
QCOM           362489          162094
NOW            357387          322216
GOOG           351011          448546
AMAT           349499          185965
ORCL           332662          147795
META           320536          152287
```

## STOCK OPT

Sample day: `2026-05-14`

### Feed Summary

| Feed | File Path | Symbol Count | Total Rows | Mean | Median | P90 | P99 | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| trades | `data/raw_data/stock/options_sp500/trades/2026/05/2026-05-14.csv.gz` | 130,802 | 4,012,240 | 30.67 | 3.00 | 37.00 | 401.99 | 59,966 |
| quotes | `data/raw_data/stock/options_sp500/quotes/2026/05/2026-05-14.csv.gz` | 533,375 | 2,297,747,750 | 4,307.94 | 1,146.00 | 8,679.60 | 59,777.60 | 294,274 |

### trades

```text
describe:
count    130802.000000
mean         30.674149
std         369.334761
min           1.000000
25%           1.000000
50%           3.000000
75%          11.000000
90%          37.000000
99%         401.990000
max       59966.000000

update-count distribution:
trades_updates
(-0.001, 1.0]            36984
(1.0, 5.0]               44479
(5.0, 10.0]              16038
(10.0, 50.0]             23297
(50.0, 100.0]             4703
(100.0, 500.0]            4269
(500.0, 1000.0]            543
(1000.0, 5000.0]           416
(5000.0, 10000.0]           48
(10000.0, 100000.0]         25
(100000.0, 1000000.0]        0
(1000000.0, inf]             0
```

Top 20 symbols:

```text
O:NVDA260515C00235000    59966
O:TSLA260515C00450000    40259
O:NVDA260515C00240000    38428
O:NVDA260515C00237500    32487
O:NVDA260515C00232500    28263
O:TSLA260515C00445000    26160
O:NVDA260515C00230000    20636
O:TSLA260515C00460000    19882
O:TSLA260515C00447500    18820
O:NVDA260515P00230000    17617
O:NVDA260515C00245000    16184
O:AAPL260515C00300000    16155
O:TSLA260515C00455000    15814
O:AVGO260515C00450000    15670
O:TSLA260515P00440000    14153
O:NVDA260515P00232500    14102
O:TSLA260515P00445000    13766
O:NVDA260515P00235000    13064
O:TSLA260515C00440000    12579
O:TSLA260515C00470000    12361
```

### quotes

```text
describe:
count    533375.000000
mean       4307.940473
std       10949.154794
min           1.000000
25%         292.000000
50%        1146.000000
75%        3059.000000
90%        8679.600000
99%       59777.600000
max      294274.000000

update-count distribution:
quotes_updates
(-0.001, 1.0]              6297
(1.0, 5.0]                29948
(5.0, 10.0]               19661
(10.0, 50.0]              25165
(50.0, 100.0]             10724
(100.0, 500.0]            84731
(500.0, 1000.0]           73709
(1000.0, 5000.0]         200111
(5000.0, 10000.0]         35047
(10000.0, 100000.0]       47218
(100000.0, 1000000.0]       764
(1000000.0, inf]              0
```

Top 20 symbols:

```text
O:NVDA260515P00227500    294274
O:NVDA260515C00245000    286534
O:NVDA260515P00230000    279494
O:NVDA260515P00225000    222591
O:NVDA260515C00240000    213998
O:NVDA260515C00237500    200238
O:NVDA260515P00222500    195878
O:NVDA260515P00232500    179316
O:NVDA260515P00220000    160885
O:NVDA260515C00250000    159878
O:NVDA260515C00230000    151346
O:TSLA260515P00425000    148459
O:TSLA260515C00467500    147811
O:TSLA261218P00465000    147453
O:TSLA261218P00455000    147170
O:TSLA260515P00427500    146272
O:TSLA260717C00445000    145776
O:TSLA260515C00472500    145385
O:NVDA260522C00235000    145212
O:TSLA260821C00460000    145130
```

### Combined Coverage

```text
total unique symbols: 533411
symbols with both trades and quotes: 130766
symbols with trades only: 36
symbols with quotes only: 402609

combined per-symbol updates preview:
                       trades_updates  quotes_updates
O:NVDA260515C00235000           59966          135129
O:TSLA260515C00450000           40259           87288
O:NVDA260515C00240000           38428          213998
O:NVDA260515C00237500           32487          200238
O:NVDA260515C00232500           28263          110818
O:TSLA260515C00445000           26160           69682
O:NVDA260515C00230000           20636          151346
O:TSLA260515C00460000           19882          107906
O:TSLA260515C00447500           18820           77668
O:NVDA260515P00230000           17617          279494
O:NVDA260515C00245000           16184          286534
O:AAPL260515C00300000           16155           30274
O:TSLA260515C00455000           15814           90781
O:AVGO260515C00450000           15670           20667
O:TSLA260515P00440000           14153           76923
O:NVDA260515P00232500           14102          179316
O:TSLA260515P00445000           13766           77786
O:NVDA260515P00235000           13064          115693
O:TSLA260515C00440000           12579           81533
O:TSLA260515C00470000           12361          131705
```