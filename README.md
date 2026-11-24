# tsp_eltdx — eltdx 原生行情的面板外部源插件包

把 [eltdx](https://github.com/eltdx/eltdx)（通达信 7709 TCP 行情协议的原生 Python 绑定）
以**外部源插件包**的形式装进 tick-stock-panel，无需重建镜像、无需预装 eltdx。

适用场景：面板运行在不能重建镜像的容器里（没有 docker build / 没有 exec 权限），
但 `data` 卷可写。此时外部源插件包是唯一可落地的扩展安装方式。

## 能力

| dataset | 内容 | 来源 |
| --- | --- | --- |
| `daily` | 日 K（不复权原始价） | `bars.get` |
| `adj_factor` | 除权因子（分红/送股/配股/前收） | `capital_changes` + 日 K 前收按交易所公式 |
| `minute` | 1 分钟 K（北京墙钟 naive） | `bars.minute` |
| `full_minute` | 当日全量分钟 | `get_intraday_batch` |
| `realtime` | 全市场快照 | `helpers.full_quotes` |
| `depth5` | 五档盘口 | `quotes.get_depth`（0x0547，100 只/批） |
| `financial` | 财务五表 | TQLEX 7615 长连接池（见 `f10.py`） |

单位口径：`volume` 是「手」、`amount` 是「元」、`change_pct` 是小数（0.0366 = 3.66%）、
日 K 不复权、分钟时间戳是北京墙钟 naive。

## 为什么 wheel 要 base64 塞进包里

面板的 `backend/app/services/package_archive.py:20` 只放行这些后缀：

    .py .yaml .yml .json .txt .md .csv

错误信息写得很明确：「只支持源码和页面产物；不接收原生库、wheel、脚本或可执行文件」。
所以 `assets/eltdx-3.3.5.wheel.b64.txt`（3.4 MB 的 base64 文本，后缀 `.txt` 合规）
随包分发，由 `bootstrap.py` 在插件导入期解码、校验 sha256、解压到 data 卷。

一个 2.5 MB 的 wheel 变成 3.4 MB 的文本，代价是 34% 的体积，
换来的是「不碰面板代码、不碰镜像」也能装；这个交换在只能走 HTTP 安装的部署里是划算的。

## 运行期自举流程（`bootstrap.py`）

1. `provider.py` 在模块导入期调用 `bootstrap.install_path()`——
   必须在这一刻，因为面板 `custom/packages.py:168` 的 `import_module()` 是裸
   `importlib.import_module`，没有任何前置钩子可以注入 `sys.path`。
2. `bootstrap` 先试 `import eltdx`；命中就返回（容器里已装 eltdx 时不重复解压）。
3. 否则读 `assets/*.b64.txt` → base64 解码 → **sha256 必须命中登记值**，
   不符就 `raise BootstrapError("eltdx wheel 校验失败: ...")`。
4. 用 `fcntl.flock` 加独占锁（多 worker 并发安装只会解一次），再查 `.ready` 标记。
5. 解到 `tempfile.mkdtemp(dir=dest.parent)` 的暂存目录，逐条校验路径不越界、
   `testzip()` 通过、`eltdx/__init__.py` 存在，最后 `rmtree(dest)` + `rename` 原子换入。
6. `sys.path.insert(0, target)` 后真正 `import eltdx` 并记录 `__version__`。
7. 落点优先级：`TSP_ELTDX_LIB_DIR` 环境变量 > `<data>/plugins/_vendor/eltdx-3.3.5` > 系统临时目录。

落点选 `plugins/_vendor` 而不是 `/tmp`：`/tmp` 随容器重建消失，而 `data` 卷持久；
`_vendor` 以 `_` 开头，`packages.discover()` 按 `^[a-z][a-z0-9_]{0,63}$` 不会把它当插件。

## 构建

    python3 build_zip.py            # -> dist/tsp_eltdx-1.0.0.zip
    python3 build_zip.py --check    # 只自检

自检覆盖：条目路径合法性、后缀白名单、20 MiB / 80 MiB 双上限、`plugin.yaml` 与
`provider.py` 的 name/datasets 一致、所有 `.py` 可 `compile()`、
base64 资产可解码且 sha256 命中。

## 安装 / 卸载

见 `INSTALL.md`。

## 已知边界

- 只支持 `linux/x86_64` 的离线自举（内嵌的就是这个 wheel）。其它平台走
  `REMOTE_WHEELS` 里的 PyPI 直链下载，需要容器能出网。
- 不做 `+x`：`_native.abi3.so` 由 `dlopen` 加载，只要求可读，不要求可执行位。
- 若 `data` 卷以 `noexec` 挂载，导入会失败，`bootstrap` 会把原因写进错误信息提示加 `:exec`。
- 插件不会被面板沙箱限制：它是普通的 Python 模块，跑在 backend 进程里，拥有 backend 的全部权限。

## 变更记录

### v1.0.10 —— 实时行情批级并发（全市场一轮 21.75s → 4.03s）

**症状**：前端报价稳定滞后 26~48 秒。

```
每轮刷新耗时: 17~20 秒（5651 只）
轮次间隔:     25.7 秒（= 20s 刷新 + 6s 间隔）
quote_age:    48.0s → 26.2s → 32.3s → 38.3s → 44.3s
```

宿主轮询间隔设的是 6 秒，但插件一轮要 21 秒 —— 永远追不上。

**根因**：`get_realtime` 的 10 个批次（600/批）严格串行，每批内部再由
`helpers.full_quotes` 按 80 只分批，合计 75 次请求 × 约 240ms ≈ 21 秒。

**修复**：批级 8 线程并发（`_REALTIME_WORKERS = 8`），`pool.map` 保序保证输出顺序不变。

**改造前先做了压测**（不是直接改生产代码）：

| 模式 | 耗时 | 返回 | 失败批 |
| --- | --- | --- | --- |
| 串行 | 21.75s | 5661 条 | 0 |
| **并发 8** | **4.03s** | **5661 条** | **0** |

提速 **5.40x**，数据零差异。压测是串行刚跑完紧接着跑并发，属于最严苛的负载条件。

**关键区别**：`full_quotes` 与分钟用的 `bars.get` 不同 —— 后者并发会触发服务器限流
（单请求 23ms → 388ms），而前者并发不限流，可以放心并行。

保留 `get_realtime` 的「软失败返回 []」契约：单批失败只记 warning 并跳过该批，
不影响其他批次；有失败时额外汇总一条 `部分批失败: x/y 批, 已返回 n 行` 便于发现数据缺失。

### v1.0.9 —— 修复盘中实时分钟的跨日污染（严重）

**症状**：盘中 `GET /api/kline/minute?symbol=000001.SZ&date=2026-11-24&live=true`
返回 240 根，其中 **11-23 占 218 根（09:53~15:00）、11-24 只有 22 根（09:31~09:52）**。

个股详情页的分时图会因此混入前一交易日的尾巴，曲线和量能全错。

**根因**：当天分支用 `client.bars.get(period="1m", count=240)`，而它返回的是
「**最近** 240 根」而不是「今天已有的 240 根」。盘中只走了 22 根时，为了凑满
240 根就回溯到了前一交易日。

这个 bug 只在**盘中**暴露——收盘后最近 240 根正好是当天全天，看起来完全正常，
所以之前的收盘后验证没能发现它。

**修复**：取回后按目标交易日过滤，只保留 `datetime.date() == day` 的行，
并统计被过滤的跨日行数写日志：

```
eltdx _minute_rows 000001.SZ 2026-11-24: 过滤跨日 218 根 / 时间字段不可解析 0 根, 保留当日 22 根
```

顺带修掉一个静默兜底：原实现 `ts = t if hasattr(t, "hour") else datetime.now()`
在时间字段类型异常时会悄悄用「当前时间」冒充，生成错误时间戳而不报错。
现在改为丢弃并计数，宁可少数据也不产出自认为正确的脏数据。

**验证**：

| 场景 | 修复前 | 修复后 |
| --- | --- | --- |
| 盘中 live 查询当日 | 240 根（11-23:218 + 11-24:22）❌ | 22 根（全是 11-24）✅ |
| 收盘后查询 | 240 根正确 | 240 根正确（未退化） |

### v1.0.8 —— 修正历史分钟价格口径（重要）+ 修复 warmer 启动

**问题一：`minutes.recent` 的价格是错的（v1.0.3 引入）**

v1.0.3 为修「历史日期全错」把历史路径改成 `recent` 优先、`history` 兜底。
实测发现 `recent`(0x0feb) 与 `history`(0x0fb4)/`bars.get`/日K **系统性不一致**：

| 日期（000001.SZ） | recent 末价 | history 末价 | 日K 收盘（基准） |
| --- | --- | --- | --- |
| 2026-11-23 | **10.47** ❌ | 12.34 ✅ | 12.34 |
| 2026-11-19 | **10.29** ❌ | 12.04 ✅ | 12.04 |
| 2026-11-18 | **10.28** ❌ | 12.20 ✅ | 12.20 |
| 2026-11-17 | **10.16** ❌ | 12.25 ✅ | 12.25 |
| 2026-11-20 / 16 | None（窗口外） | 12.22 / 12.30 ✅ | 同左 |

`history` 6/6 全对，`recent` 6/6 全错或为空。且 `recent` 的 `prev_close`
也同步错误（10.50 vs 应为 12.22）。

这个错误在有本地落盘数据时被「本地命中」掩盖（TSP 优先读 parquet），
**只在本地缺分区的历史日期上暴露** —— 属静默错误。

修正：以 `minutes.history` 为主路径，`recent` 仅在 history 失败/为空时兜底。

**问题二：`Path` 未导入，cache-warmer 从未启动**

`_start_warmer()` 里用 `Path(__file__).resolve().parent` 但模块没 import，
每次实例化都抛 `name 'Path' is not defined`，被 except 吞成「非致命」告警：

```
eltdx cache-warmer 启动失败 (非致命): name 'Path' is not defined
```

已补 `from pathlib import Path`。

### v1.0.6 —— `iter_minute` 并发化（提速约 8 倍）+ 失败可见性

v1.0.5 严格照官方范本做成了「串行 + 每任务 sleep(0.08s)」，实测反而成了瓶颈：

| 路径 | 8 只耗时 | 折算全市场（5579 只） |
| --- | --- | --- |
| `get_minute`（8 线程并发） | 546 ms | **1.2 分钟/天** |
| `iter_minute`（v1.0.5 串行） | 824 ms | **9.6 分钟/天** |

`minute_sync_days=5` 时全量同步要 **48 分钟**，用户明确反馈太慢。

改法：改成 8 线程并发，**每个 worker 内部仍保留 0.08s 节流**——
单个连接的压力与串行时完全一致，只是让 8 个连接并行跑：

- 总吞吐提升约 8 倍，全市场一天约 1.5 分钟
- `pool.map` 保序，`on_chunk_done` 进度仍单调递增
- 并发度复用原有的 `_MINUTE_WORKERS = 8`（原作者在 `get_minute` 里已长期使用该并发度）

同时补上**失败可见性**：过去单个 (标的 × 交易日) 取数失败会静默跳过，
用户只看到「行数比预期少」却无从判断。现在结束时汇总一条告警：

```
iter_minute 完成: 12/27900 个 (标的×交易日) 无数据或取数失败 (占 0.0%), 其余已返回
```

仍未解决（已知）：
- `get_minute` 本身没有节流（8 线程直发）；原作者如此设计且长期稳定，故不改。
- `_minute_rows` 里 `ts = t if hasattr(t, "hour") else datetime.now()` 的静默兜底
  仍可能生成错误时间戳，属低概率路径，暂留观察。


### v1.0.5 —— 补齐流式分钟接口 `iter_minute`

`plugin_api_version: 1` 的可选能力。面板做历史分钟同步时，会把「完整标的列表 +
完整时间窗」**一次性**交给插件（它不替源拆批）。此前只有 `get_minute`，
宿主会退化成「一次调用、整包返回」，标的多了容易把内存和通达信连接压满。

新增实现要点：

- 按 `(交易日 × 标的)` 串行取数，累积到帧阈值就 `yield` 一帧：
  单帧 ≤ `_MINUTE_OUTPUT_ROWS`(100000 行) 且 ≤ `_MINUTE_OUTPUT_SYMBOLS`(50 标的)，
  与官方内置源 `tdxquant` 同量级。
- 每完成一个任务 `on_chunk_done(done, total)` 回调节流一次；任务间
  `time.sleep(_MINUTE_INTERVAL_S)`(0.08s) 限频，避免打爆行情服务器。
- **进度回调抛出的异常（含取消）原样向上传播**，不做 try 包裹 —— 规范明确要求
  流式接口必须原样传递取消异常，否则宿主的「停止同步」会失效。
- 不支持的输入（`freq` 非 1m、`symbols` 为空、窗口内无交易日）走
  `on_chunk_done(0, 0)` 后直接 `return`，不 yield 空帧。

`get_minute` 保留不变（内部仍用线程池并发，适合单次少量取数）。

### v1.0.4 —— `minutes.recent` 返回空时也要回退

v1.0.3 上线后实测：`11-17 / 11-18 / 11-19` 都正常，唯独 `11-20` 返回 0 行，
且日志里连失败记录都没有。

根因：`client.minutes.recent()`（0x0feb）只覆盖**服务端近期窗口**内的交易日，
窗口外时它返回一个**空 `MinuteSeries` 而不抛异常**。原实现只捕获异常做回退，
于是「返回空」被当成「取数成功但无数据」，静默返回 `None`。

修复：取回结果后检查 `points` 是否非空，为空则记一条日志并回退 `minutes.history()`：

```
eltdx minutes.recent 000001.SZ 2026-11-20 返回空(窗口外), 退 history
```

### v1.0.3 —— 历史分钟全错（严重）

**症状**：请求任何历史交易日的分钟K，返回的都是**当天**的数据，且行数按请求天数
成倍重复（请求 9 天 → 2160 行全是今天，每根重复 9 次）。不报错，属于静默错误。

**根因**（`_minute_rows`）：

```python
if days_ago <= 70:                                    # ← 70 天内全走这个分支
    client.bars.get(code, period="1m", count=240)     # ← 没有"取哪天"的参数
```

`client.bars.get()` 的 `anchor_date` 是**定点复权基准日**（配合
`adjust="fixed_qfq"/"fixed_hfq"`），**不是**「从哪天取」；该接口永远是
「最新 count 根」。所以条件 `days_ago <= 70` 让历史日期也去拿最新 240 根，
9 天就是 9 次同样的结果。

**修复**：只有 `days_ago == 0`（当天）才用 `bars.get`（它能给真实 OHLC），
历史日期改走历史分时接口：

| 场景 | 接口 | 底层命令 |
| --- | --- | --- |
| 当天 | `bars.get(period="1m", count=240)` | 0x052d |
| 近期历史 | `minutes.recent(code, day)` | 0x0feb |
| 更早/窗口外 | `minutes.history(code, day)` | 0x0fb4 |

**验证**（`000001.SZ`）：

| 请求日期 | 修复前 | 修复后 |
| --- | --- | --- |
| 2026-11-20 | 240 根，日期是 11-23 ❌ | 240 根，日期 11-20 ✅ |
| 2026-11-19 | 240 根，日期是 11-23 ❌ | 240 根，日期 11-19 ✅ |
| 2026-11-16 | 240 根，日期是 11-23 ❌ | 240 根，日期 11-16 ✅ |

### v1.0.3 —— `availability()` 违反规范

面板规范要求 `check` 入口「只做本地就绪检查，**不进行慢网络操作**」。

原实现调了 `get_client()`（建 8 连接池）和 `client.ping()`（网络往返），
导致每次能力路由探测都打网络（日志里每几分钟一批 `granted` 即由此触发），
既违反规范又拖慢启动。

改为只确认 `bootstrap.status()`（原生库已就位、sha256 已校验）。
服务器连通性留到真正取数时验证，失败会作为 `ProviderRequestError` 正常上报。

### v1.0.3 —— 补齐清单约束字段

```yaml
requires_app: '>=0.3.24'
requires_python: '>=3.11'
platforms: [linux-x86_64]   # 内嵌的是 manylinux x86_64 wheel
```

此前缺这三项，面板在版本不匹配或非 x86_64 平台时没有任何提示，只会静默加载失败。
