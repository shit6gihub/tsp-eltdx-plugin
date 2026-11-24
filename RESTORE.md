# TSP ELTDX 数据源插件 —— 仓库说明与恢复指南

本仓库是 **tick-stock-panel (TSP) 的 `tsp_eltdx` 外部数据源插件**源码镜像。
目的：即使开发环境（DSH 会话、记忆、本地工作副本）全部丢失，也能凭本仓库从零恢复。

- 当前版本：**v1.0.10**
- 上游依赖：`eltdx` 3.3.5（通达信 7709 TCP 原生绑定）
- 目标宿主：TSP ≥ 0.3.24（plugin_api_version 1），Python ≥ 3.11，linux-x86_64
- 详细变更记录见 [README.md](README.md) 的「变更记录」章节

---

## 一、这是什么

插件让 TSP 直接连通达信行情服务器（7709 协议），接管七类数据：

| dataset | 内容 |
| --- | --- |
| `daily` | 日 K（不复权原始价） |
| `adj_factor` | 除权因子 + 分红/送股/配股明细 |
| `minute` | 1 分钟 K |
| `full_minute` | 当日全量分钟 |
| `realtime` | 全市场实时快照 |
| `depth5` | 五档盘口 |
| `financial` | 财务五表 |

---

## 二、从零恢复到一台新机器

### 1. 拿到完整文件

仓库里 `assets/eltdx-3.3.5.wheel.b64.txt` **是生成物，不在版本库中**，需先生成：

```bash
git clone <本仓库> tsp_eltdx && cd tsp_eltdx
python3 tools/make_assets.py        # 从 PyPI 下载 wheel、校验 sha256、生成 base64
```

### 2. 打包成 TSP 可安装的 ZIP

**关键**：ZIP 根目录必须直接含 `plugin.yaml`，不能多套一层目录。

```bash
zip -r ../tsp_eltdx.zip . -x '__pycache__/*' '*.pyc' 'cache/*' -x '.git/*'
```

包体积约 2.5 MB（压缩后），满足 TSP 的 20 MiB 上限。

### 3. 安装进 TSP

```bash
P=/vol4/1000/ydyp/tick-stock-panel          # 按实际路径改
PW=$(grep '^AUTH_PASSWORD' "$P/.env" | cut -d= -f2- | tr -d "\"'")

# 登录拿 cookie
curl -c /tmp/cj.txt -X POST http://127.0.0.1:3018/api/auth/login \
  -H 'Content-Type: application/json' -d "{\"password\":\"$PW\"}"

# 预检（errors 必须为空），digest 只能从 stdout 取
curl -b /tmp/cj.txt -X POST http://127.0.0.1:3018/api/custom/source-packages/inspect \
  -F "file=@../tsp_eltdx.zip" > /tmp/insp.json
DIGEST=$(python3 -c "import json;print(json.load(open('/tmp/insp.json')).get('digest',''))")

# 安装（暂存）+ 重启生效
curl -b /tmp/cj.txt -X POST http://127.0.0.1:3018/api/custom/source-packages/install \
  -F "file=@../tsp_eltdx.zip" -F "digest=$DIGEST"
docker compose -p tickstockpanel restart      # 或用飞牛 Docker 界面重启
```

### 4. 配置数据源路由

插件装好、重启后，到 **设置 → 数据源**：

1. 卡片 `eltdx 原生行情 (通达信 7709)` 应显示「已就绪」
2. 把需要的能力切到该源。**推荐全切**：

```jsonc
// data/user_data/preferences.json 里的对应键
"daily_data_provider": "tsp_eltdx",
"adj_factor_provider": "tsp_eltdx",
"minute_data_provider": "tsp_eltdx",
"full_minute_data_provider": "tsp_eltdx",
"realtime_data_provider": "tsp_eltdx",
"depth5_data_provider": "tsp_eltdx",
"financial_data_provider": "tsp_eltdx"
```

> ⚠️ **别指向 `tickflow`**：没有 TickFlow API Key 时 `mode=none`，取不到任何数据。
> 也别指向已废弃的 HTTP 源 `eltdx`（那套指向 `:3022` 的配置早已失效）。

### 5. ⚠️ 必做：分钟口径迁移

**这是最容易漏、后果最隐蔽的一步。**

插件返回的是**原始价**（不复权）。TSP 通过 `data/kline_minute/.raw_basis` 标记判断
「分区是原始口径」，从而在**读取时**做复权投影。标记缺失 → TSP 以为分钟已是前复权
→ 不做投影 → **除权日之前的分钟价与日K 最多差 5.57%，且不报任何错**。

```bash
curl -b /tmp/cj.txt -X POST http://127.0.0.1:3018/api/kline/minute-migrate
# 期望: {"status":"ok","partitions":N,"converted_symbols":M,"failed":0,"marked":true}
```

同样处理 ETF 目录（即使暂时没有 ETF 分钟数据，预防性建标记）：

```bash
touch "$P/data/kline_etf_minute/.raw_basis"
```

**验证**（除权日之前也必须一致）：

```bash
# 000001.SZ 除权日 2026-06-12 / 2026-09-24；取除权前的 2026-09-10 比较
curl -b /tmp/cj.txt "http://127.0.0.1:3018/api/kline/minute?symbol=000001.SZ&date=2026-09-10"
curl -b /tmp/cj.txt "http://127.0.0.1:3018/api/kline/daily?symbol=000001.SZ&days=400"
# 两者的收盘价必须一致（差异 < 0.01%）
```

---

## 三、验证装好了没有

| 项 | 判据 |
| --- | --- |
| 版本 | `data/plugins/tsp_eltdx/state.json` 的 `active` = 目标版本 |
| 能力 | 日志有 `custom source 'tsp_eltdx' provides dataset … granted`，能力路由显示「可用」 |
| 日K | `/api/kline/daily?symbol=000001.SZ&days=10` 有数据、OHLC 自洽 |
| 分钟 | 当日 + 除权日之前的历史都对得上日K（见上一步验证） |
| 实时 | `/api/intraday/status` 股票数 > 5600、`quote_age` < 25 秒 |
| 五档 | 盘中直调插件 `get_depth_batch`，买档递减、卖档递增、买一 ≤ 卖一 |
| 财务 | `/api/financials/income?symbol=000001.SZ&latest_only=true` 有值 |

盘中直调插件的方法（需要 docker 权限时用 fnos-cli 造一个挂 docker.sock 的容器）：

```bash
docker exec TickFlow_Stock_Panel /app/.venv/bin/python - <<'PY'
import sys,glob,os,importlib.util
V=sorted(glob.glob('/app/data/plugins/tsp_eltdx/versions/*/provider.py'))[-1]
s=importlib.util.spec_from_file_location("p",V,submodule_search_locations=[os.path.dirname(V)])
m=importlib.util.module_from_spec(s); sys.modules["p"]=m; s.loader.exec_module(m)
print(m.availability()); print(m.EltdxProvider().get_depth_batch(['000001.SZ']))
PY
```

> 注意用 `/app/.venv/bin/python`，系统 python 里没有 polars。

---

## 四、踩过的坑（别再踩）

| # | 问题 | 结论 |
| --- | --- | --- |
| 1 | 历史分钟全错，返回的永远是当天数据且重复 | `bars.get(anchor_date=…)` 的 `anchor_date` 是**定点复权基准日**，不是「取哪天」；该接口永远返回最新 N 根 |
| 2 | 历史分钟价格系统性偏低 | `minutes.recent()`(0x0feb) 价格与 `history()`/日K 不一致，**历史必须用 `minutes.history()`(0x0fb4)**，`recent` 仅兜底 |
| 3 | 盘中查当日分钟混入前一交易日尾巴 | `bars.get(count=240)` 是「**最近** 240 根」，盘中只走了几十根时会回溯；必须按 `datetime.date() == 目标日` 过滤 |
| 4 | 除权日之前的分钟与日K 差 2~5.6%，静默 | 缺 `data/kline_minute/.raw_basis` 标记，见上文「必做」 |
| 5 | 实时行情报价滞后 26~48 秒 | `get_realtime` 原为 10 批串行（全市场 21.75s）。`helpers.full_quotes` **并发不限流**（与 `bars.get` 不同），8 线程并发后降到 4s |
| 6 | 收盘后同步空跑 5584 次请求 | 宿主增量同步的 `start_time` 是「本地已有最新分钟」（收盘后=15:00），窗口落在交易时段外；按时间窗过滤可整批跳过 |
| 7 | `cache-warmer` 从未启动 | `_start_warmer` 用了 `Path` 却没 import，异常被 except 吞成「非致命」 |
| 8 | `availability()` 拖慢启动 | 面板规范要求 check 只做**本地**就绪检查，禁止慢网络操作；原实现建连接池 + ping |
| 9 | `minutes.recent` 窗口外返回空却不抛异常 | 必须检查 `points` 非空才用，否则静默拿不到数据 |
| 10 | `bars.get` 并发会触发服务器限流（单请求 23ms→388ms） | 分钟取数并发度要克制；`full_quotes` 则不受此限 |

---

## 五、目录结构

```
.
├── plugin.yaml          # 清单：name/version/entry/datasets/requires_*
├── provider.py          # Provider 实现（七类数据集 + iter_minute 流式）
├── client.py            # TdxClient 单例连接池 + 代码互转
├── bootstrap.py         # 运行期自举：解 base64 → 校验 sha256 → 解压 → sys.path
├── f10.py               # 财务五表（TQLEX 7615 长连接池 + 12h 缓存）
├── warmer_loop.py       # 缓存预热后台线程
├── warmer/              # 预热所需的 gaps 数据抓取
├── assets/
│   └── README.md        # wheel base64 为生成物，见 tools/make_assets.py
├── tools/
│   └── make_assets.py   # 生成 assets/eltdx-3.3.5.wheel.b64.txt
├── README.md            # 插件说明 + 完整变更记录
└── RESTORE.md           # 本文件
```
