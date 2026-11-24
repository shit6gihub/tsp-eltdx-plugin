<div align="center">

# TSP · ELTDX 数据源插件

**让 [tick-stock-panel](https://github.com/shy3130/tick-stock-panel) 直连通达信行情服务器（7709 协议）**

一个进程内外部源插件：不重建镜像、不预装原生库、不依赖付费 API，
把日 K / 除权 / 分钟 / 全量分钟 / 实时 / 五档 / 财务七类数据接进 TSP。

[![Version](https://img.shields.io/badge/version-1.0.11-blue?style=for-the-badge)](https://github.com/shit6gihub/tsp-eltdx-plugin/releases)
[![Plugin API](https://img.shields.io/badge/plugin__api-1-2ea44f?style=for-the-badge)]()
[![Python](https://img.shields.io/badge/python-%E2%89%A53.11-3776AB?style=for-the-badge&logo=python&logoColor=white)]()
[![Platform](https://img.shields.io/badge/platform-linux--x86__64-lightgrey?style=for-the-badge&logo=linux&logoColor=white)]()
[![Host](https://img.shields.io/badge/host-TSP%20%E2%89%A50.3.24-orange?style=for-the-badge)]()
[![License](https://img.shields.io/badge/license-Non--Commercial-red?style=for-the-badge)](LICENSE)

[数据能力](#-数据能力) · [安装](#-安装) · [设计要点](#-设计要点) · [变更记录](CHANGELOG.md) · [恢复指南](RESTORE.md) · [许可](LICENSE)

</div>

---

## 📖 这是什么

TSP 的插件机制要求「外部数据源」以纯源码包形式交付，而 eltdx 是 Rust 原生库。
本项目把 eltdx 的 wheel 转成 base64 文本随包分发（`.txt` 后缀才能过 TSP 的包检查），
在插件首次加载时解码、校验 sha256、解压到数据卷并注入 `sys.path` —— 于是
**不需要重建镜像，也不需要预装 eltdx**。

运行时它跑在 TSP 后端进程里，直接通过 7709 TCP 协议连通达信公开行情服务器。

## ✨ 数据能力

| 数据集 | 内容 | 底层接口 |
| --- | --- | --- |
| `daily` | 日 K（不复权原始价） | `bars.get` · 0x052d |
| `adj_factor` | 除权因子 + 分红/送股/配股明细 | `corporate.capital_changes` |
| `minute` | 1 分钟 K | `bars.get` / `minutes.history` · 0x0fb4 |
| `full_minute` | 当日全量分钟 | `get_minute` 流式 |
| `realtime` | 全市场实时快照（5600+ 只） | `helpers.full_quotes` |
| `depth5` | 五档盘口 | `quotes.get_depth` · 0x0547 |
| `financial` | 财务五表 | TQLEX 7615 长连接池 |

实测覆盖：**A 股主板 / 创业板 / 科创板 / 北交所 / ETF / 指数**。

## 🙏 上游与相关项目

本项目站在两个开源项目之上，特此致谢：

| 项目 | 作者 | 作用 |
| --- | --- | --- |
| [**eltdx**](https://github.com/electkismet/eltdx) | [@electkismet](https://github.com/electkismet) | 通达信 7709 协议的原生 Python 绑定（Rust）。本插件的全部行情能力来自它 |
| [**tick-stock-panel (TSP)**](https://github.com/shy3130/tick-stock-panel) | [@shy3130](https://github.com/shy3130) | 自托管的 A 股量化工作台，本项目是它的外部数据源插件 |

插件本身遵循 TSP 的 [外部数据源插件规范](https://github.com/shy3130/tick-stock-panel/blob/main/docs/external-data-source-packages.md)：
`plugin_api_version: 1`、相对导入、构造函数无网络访问、`availability()` 只做本地检查。

## 🚀 安装

```bash
git clone https://github.com/shit6gihub/tsp-eltdx-plugin.git
cd tsp-eltdx-plugin
zip -r ../tsp_eltdx.zip . -x '__pycache__/*' '*.pyc' 'cache/*' '.git/*'
```

然后上传到 TSP 的「设置 → 导入与备份」，预检通过后安装、重启容器生效。

> 完整步骤（含数据源路由配置、**必做的分钟口径迁移**、验证清单）见 **[RESTORE.md](RESTORE.md)**。

**要求**：TSP ≥ 0.3.24 · Python ≥ 3.11 · linux-x86_64

## 🔧 设计要点

几个踩坑换来的关键结论，改代码前建议先看：

- **历史分钟必须走 `minutes.history()`（0x0fb4）**。`minutes.recent()`（0x0feb）的价格与
  `history`/日 K 系统性不一致，且窗口外返回空却不抛异常。
- **`bars.get()` 的 `anchor_date` 不是「取哪天」**，而是定点复权基准日；该接口永远返回
  最新 N 根。盘中只走了几十根时会回溯到前一交易日，必须按 `datetime.date()` 过滤。
- **分钟口径靠 `.raw_basis` 标记**。插件存原始价，TSP 读取时投影复权；标记缺失会让
  除权日之前的分钟价格静默偏差 5%+。
- **并发行为不统一**：`helpers.full_quotes` 并发不限流（8 线程 5.4× 提速），
  但 `bars.get` 并发会触发服务器限流（单请求 23ms → 388ms）。

更多细节见 [RESTORE.md § 四、踩过的坑](RESTORE.md)。

## 📁 目录结构

```
.
├── plugin.yaml          # 清单：name/version/entry/datasets/requires_*
├── provider.py          # Provider 实现（七类数据集 + iter_minute 流式）
├── client.py            # TdxClient 单例连接池 + 代码互转
├── bootstrap.py         # 运行期自举：解 base64 → 校验 sha256 → 解压 → sys.path
├── f10.py               # 财务五表（TQLEX 7615 + 12h 缓存）
├── warmer_loop.py       # 缓存预热后台线程
├── warmer/              # 预热所需 gaps 抓取（竞价 / 连板 / 龙虎榜）
├── assets/              # 内嵌 wheel 的 base64（3.4 MB）
├── tools/make_assets.py # 从 PyPI 重建该 base64（含 sha256 校验）
├── CHANGELOG.md         # 完整变更记录
└── RESTORE.md           # 从零恢复指南 + 踩坑清单
```

## 📄 许可

本项目采用 **[TSP-ELTDX 非商业使用许可](LICENSE)**（`LicenseRef-TSP-ELTDX-NC-1.0`）：
允许个人学习、研究与非商业自用，允许阅读/使用/复制/修改/非盈利分发；
**禁止任何商业用途**（含付费服务、生产服务、转售、盈利性再分发、行情数据售卖、自动化交易服务）。

之所以不是 MIT，是因为本插件**内嵌了 `eltdx` 的二进制**，而 eltdx 采用的是
[ELTDX Non-Commercial Learning and Data Research License](https://github.com/electkismet/eltdx/blob/main/LICENSE)
——严格非商业，且**不属于 OSI 认可的开源许可**。两者叠加，取更严格者。

第三方组件与各自的许可见 **[NOTICE](NOTICE)**：

| 组件 | 关系 | 许可 |
| --- | --- | --- |
| [eltdx](https://github.com/electkismet/eltdx) | 内嵌二进制（base64） | ELTDX 非商业许可 |
| [tick-stock-panel](https://github.com/shy3130/tick-stock-panel) | 宿主，不含其代码 | MIT |
| 通达信行情数据 | 运行时外部拉取 | 数据提供方条款 |

> 行情数据可能存在延迟、缺失或错误，**不得作为投资决策的唯一依据**。
