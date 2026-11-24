"""eltdx(通达信 7709 行情协议)内置数据源 provider 骨架。

方法签名对齐 data_providers/custom/GenericHTTPProvider 与 docs/plugin-development.md,
loader 扫到 plugin.yaml 后实例化本类并把各 service 的路由切过来。

发行方式: 作为外部源插件包 (data/plugins/tsp_eltdx) 安装, 名单 white-list 只有 7 个数据集,
因为 packages.DATASETS 不含 instruments。原生库 eltdx 以 base64 文本随包分发, 由
bootstrap.py 在导入期解码、校验 sha256、解压到 data 卷再 sys.path.insert —— 这样
在不能重建镜像的部署里也能装 (详细取舍见 bootstrap.py 顶部注释)。

实现数据集:
  - daily        日K (不复权原始价, count 模式)
  - adj_factor   除权因子 (capital_changes 事件 + 日K前收按交易所公式)
  - minute       1m 分钟K (北京墙钟 naive)
  - full_minute  当日全量分钟 (get_intraday_batch / get_intraday_latest)
  - realtime     全市场快照 (helpers.full_quotes)
  - depth5       五档盘口 (quotes.get_depth, 快照只有 1 档)
  - financial    财务五表 (f10.py, TQLEX 7615 长连接池)

单位与口径:
  - volume 单位「手」(eltdx 的 volume_lots/total_hand 直接是手, 不再除 100)
  - amount 单位「元」
  - change_pct 小数制 (0.0366 = 3.66%)
  - 日K 为不复权原始价, 复权由 adj_factor + indicators.pipeline 处理
  - 分钟 datetime 为北京墙钟 naive
"""
from __future__ import annotations

import logging
from pathlib import Path
import math
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from datetime import time as _clock
from typing import Any, Iterator
from zoneinfo import ZoneInfo

import polars as pl

# 注意顺序: bootstrap 必须在任何会 import eltdx 的模块之前跑起来。
# 面板的 custom/packages.py:168 import_module() 是裸 importlib.import_module,
# 没有任何前置钩子可以注入 sys.path, 所以自举只能放在本模块的导入期完成。
from . import bootstrap, f10
from .client import close_client, get_client, to_std, to_tdx

# 导入期自举结果。这里刻意不抛: 抛了 packages.build_provider() 只会看到
# "插件加载失败，请检查开发文档中的 Provider 契约", 真正的失败原因(wheel 校验失败 /
# data 卷不可写 / noexec)就丢了。改为存下原因, 由 availability() 原样报给面板。
_BOOTSTRAP_PATH: str | None = None
_BOOTSTRAP_ERROR: str | None = None
try:
    _BOOTSTRAP_PATH = str(bootstrap.install_path())
except bootstrap.BootstrapError as _exc:  # noqa: BLE001
    _BOOTSTRAP_ERROR = str(_exc)

logger = logging.getLogger(__name__)

_BEIJING = ZoneInfo("Asia/Shanghai")

# 外部源插件包的 DATASETS 白名单只有这 7 项(packages.py:32); instruments 不在其中,
# 因为代码表同步只走内置 tickflow 源, 这里保留 get_instruments() 方法供自检/排错。
_DATASETS = (
    "daily",
    "adj_factor",
    "minute",
    "full_minute",
    "realtime",
    "depth5",
    "financial",
)

_SNAPSHOT_BATCH = 600        # full_quotes 单批上限 (helpers 内部再按 80 分批)
# v1.0.10: 实时快照并发度。实测 5661 只: 串行 21.75s → 8 并发 4.03s (5.4x),
# 且两轮背靠背压测下数据零差异、零失败批 —— full_quotes 与 bars.get 不同, 并发不触发限流。
_REALTIME_WORKERS = 8
_DEPTH_BATCH = 100           # 0x0547 五档单请求上限
_HIST_MAX_COUNT = 8000       # bars.get 单次 count 上限
_ADJ_BAR_COUNT = 3000        # 除权配价用的日K根数
_MINUTE_BARS_PER_DAY = 240
_MINUTE_WORKERS = 8

# 分钟流式输出的分帧阈值(v1.0.5). 与官方内置源 tdxquant 同量级:
# 单帧不超过 10 万行 / 50 个标的, 避免一次 yield 过大压满宿主内存;
# 每取一个 (标的, 交易日) 后节流 0.08s, 防止打爆通达信服务器。
_MINUTE_OUTPUT_ROWS = 100_000
_MINUTE_OUTPUT_SYMBOLS = 50
_MINUTE_INTERVAL_S = 0.08
# ------------------------------------------------- name / 流通股本 缓存
# 名称: codes.all() 三市场合计约 4.0s, 每条 SecurityCode 都带 name, 一份缓存同时
# 覆盖股票/指数/ETF (实测 (sh,600519)->贵州茅台 / (sz,399001)->深证成指)。
# 进程内只取一次 —— 不要每轮快照都拉。
_NAME_LOCK = threading.Lock()
_NAME_MAP: dict[str, str] | None = None
_NAME_CODES: list[str] = []          # A 股全码 (sh600519 形态), 换手率分母的口径

# 流通股本: corporate.finance_batch 一次塞全市场会抛
# ConnectionClosedError(7709 TCP stream closed), 必须 80/批
# (与 eltdx helpers/core.py:930 _finance_map 一致); 5582 只 70 批实测 3.6s。
# 只对 A 股有意义: 指数/ETF 不查财务, 换手率留 None。
_SHARE_LOCK = threading.Lock()
_SHARE_MAP: dict[str, float] | None = None
_SHARE_FULL = [False]
_SHARE_TS = [0.0]
_SHARE_TTL_S = 12 * 3600.0
_FINANCE_CHUNK = 80
_SHARE_SMALL = 200                  # 少于该数量只按需补, 不做全市场权威拉取


def _name_map() -> dict[str, str]:
    """symbol -> 名称。软失败返回已取到的部分 (可能为空, 不伪造)。"""
    global _NAME_MAP
    if _NAME_MAP is not None:
        return _NAME_MAP
    with _NAME_LOCK:
        if _NAME_MAP is not None:
            return _NAME_MAP
        mapping: dict[str, str] = {}
        codes: list[str] = []
        try:
            client = get_client()
            for market in ("sh", "sz", "bj"):
                try:
                    items = client.codes.all(market) or ()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("eltdx 代码表 %s 失败: %s", market, exc)
                    continue
                for item in items:
                    code = str(getattr(item, "code", "") or "")
                    if not code:
                        continue
                    exchange = str(getattr(item, "exchange", "") or market).lower()
                    name = getattr(item, "name", None)
                    if name:
                        mapping[to_std(exchange + code)] = str(name)
                    if str(getattr(item, "category", "") or "") == "a_share":
                        codes.append(exchange + code)
        except Exception as exc:  # noqa: BLE001
            logger.warning("eltdx 名称表构建失败: %s", exc)
        _NAME_CODES[:] = codes
        _NAME_MAP = mapping
        return mapping


def _ensure_shares(codes) -> dict[str, float]:
    """确保 symbol -> 流通股本(股) 覆盖给定标的; 全市场结果缓存 12h。"""
    global _SHARE_MAP
    now = time.time()
    if _SHARE_MAP is not None and _SHARE_FULL[0] and now - _SHARE_TS[0] < _SHARE_TTL_S:
        return _SHARE_MAP
    wanted = [to_tdx(str(c)) for c in (codes or ()) if c]
    if not wanted:
        return _SHARE_MAP or {}
    with _SHARE_LOCK:
        now = time.time()
        if _SHARE_MAP is not None and _SHARE_FULL[0] and now - _SHARE_TS[0] < _SHARE_TTL_S:
            return _SHARE_MAP
        mapping = dict(_SHARE_MAP or {})
        full = len(wanted) > _SHARE_SMALL
        targets = list(wanted)
        if full:
            if not _NAME_MAP:
                _name_map()
            targets = list(_NAME_CODES) or list(wanted)
        missing = [c for c in targets if to_std(c) not in mapping]
        if missing:
            try:
                client = get_client()
                for offset in range(0, len(missing), _FINANCE_CHUNK):
                    batch = missing[offset:offset + _FINANCE_CHUNK]
                    try:
                        # 返回 FinanceBatch(.records), 不是可迭代的记录本身
                        page = client.corporate.finance_batch(batch, on_missing="skip")
                        records = getattr(page, "records", None) or ()
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("eltdx 财务批 %d 失败: %s", offset, exc)
                        continue
                    for record in records:
                        symbol = to_std(str(getattr(record, "full_code", "") or ""))
                        try:
                            shares = float(getattr(record, "circulating_shares", 0) or 0)
                        except Exception:  # noqa: BLE001
                            shares = 0.0
                        if symbol and shares > 0:
                            mapping[symbol] = shares
            except Exception as exc:  # noqa: BLE001
                logger.warning("eltdx 流通股本构建失败: %s", exc)
        _SHARE_MAP = mapping
        if full:
            _SHARE_FULL[0] = True
            _SHARE_TS[0] = time.time()
        return mapping


def _turnover_rate(symbol: str, volume_hand: float) -> float | None:
    """换手率(小数制, 0.05 = 5%) = 手 * 100 / 流通股本; 分母缺失留 None。

    权威全量缓存(_SHARE_FULL)已建立时不再逐只补拉 —— ETF/指数不在财务表里,
    否则每行一次 7709 往返, 全市场会多出上千次请求。
    """
    if not symbol or not volume_hand:
        return None
    shares = (_SHARE_MAP or {}).get(symbol)
    if shares is None and not _SHARE_FULL[0]:
        shares = _ensure_shares([symbol]).get(symbol)
    if not shares or shares <= 0:
        return None
    return round(float(volume_hand) * 100.0 / shares, 6)


def _stock_name(symbol: str) -> str | None:
    """symbol -> 名称; 取不到返回 None。"""
    if not symbol:
        return None
    return _name_map().get(symbol)


_ADJ_WORKERS = 8
_DAILY_SCHEMA = {
    "symbol": pl.String,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
}

_ADJ_SCHEMA = {
    "symbol": pl.String,
    "trade_date": pl.Date,
    "ex_factor": pl.Float64,
    "dividend": pl.Float64,
    "bonus": pl.Float64,
    "allot": pl.Float64,
    "allot_price": pl.Float64,
    "prev_close": pl.Float64,
}

_MINUTE_SCHEMA = {
    "symbol": pl.String,
    "datetime": pl.Datetime("us"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
}


@dataclass
class _EltdxConfig:
    """最小 config 对象: loader/provider_has_dataset 只认 .datasets 的键。"""

    name: str = "tsp_eltdx"
    display_name: str = "eltdx 原生 (通达信行情)"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS, True))
    path: str | None = None
    builtin: bool = True


def _to_float(value) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _compact_date(value) -> date | None:
    """YYYYMMDD / YYYY-MM-DD / date / datetime -> date。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("-", "").replace("/", "")[:8]
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except (TypeError, ValueError):
        return None


def _snapshot_ms(raw, fallback_ms: int) -> int:
    """通达信快照时间 HHMMSScc(百分秒) -> epoch 毫秒; 异常回退 fallback。"""
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return fallback_ms
    if value <= 0:
        return fallback_ms
    hh, rest = divmod(value, 1000000)
    mm, rest = divmod(rest, 10000)
    ss, cc = divmod(rest, 100)
    if not (0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60):
        return fallback_ms
    today = datetime.now(_BEIJING)
    stamp = today.replace(hour=hh, minute=mm, second=ss, microsecond=cc * 10000, tzinfo=None)
    return int(stamp.replace(tzinfo=_BEIJING).timestamp() * 1000)


def _now_dt() -> datetime:
    """当前北京时间 datetime（用于「是否已到午后」这类本地时刻判断）。"""
    return datetime.now(_BEIJING)


def _now_ms() -> int:
    return int(datetime.now(_BEIJING).timestamp() * 1000)


def _ref_price(prev_close, dividend, bonus, allot, allot_price) -> float | None:
    """交易所除权参考价公式 (每 10 股口径已在调用处折成每股)。"""
    base = _to_float(prev_close)
    if base is None or base <= 0:
        return None
    dividend = _to_float(dividend) or 0.0
    bonus = _to_float(bonus) or 0.0
    allot = _to_float(allot) or 0.0
    allot_price = _to_float(allot_price) or 0.0
    denom = 1.0 + bonus + allot
    if denom <= 0:
        return None
    return math.floor(((base - dividend + allot * allot_price) / denom) * 100 + 0.5) / 100


def _price_limit(symbol: str) -> float:
    code = str(symbol).split(".")[0]
    if code.startswith(("300", "301", "302", "688", "689")):
        return 0.20
    if code.startswith(("4", "8", "92")):
        return 0.30
    return 0.10


def availability() -> tuple[bool, str]:
    """loader 的 check 入口: 原生库就位 + 服务器可达才返回 True。

    这里必须先跑 bootstrap.status(), 因为外部插件包不会预装 eltdx: 面板的
    packages.build_provider() 会在安装时调用本函数, 返回 False 就把插件登记为
    "不可用"并用 _UnavailableProvider 接管全部取数, 之后没有任何补救入口。
    所以自举(解码 base64 wheel → 校验 sha256 → 解压 → sys.path.insert)必须发生在这里。
    status() 不抛异常, 失败时给出可执行提示而不是让整个安装流程崩掉。
    """
    ok, detail = bootstrap.status()
    if not ok:
        # 导入期已经失败过一次时, 把两次原因都带上, 便于区分"确定性失败"(sha256 不符、
        # 目录只读)和"瞬态失败"(下载超时)。
        if _BOOTSTRAP_ERROR and _BOOTSTRAP_ERROR != detail:
            return False, _BOOTSTRAP_ERROR if detail in _BOOTSTRAP_ERROR else f"{_BOOTSTRAP_ERROR}；重试: {detail}"
        return False, detail
    # 修复(v1.0.3): 面板规范要求 check 只做本地就绪检查、不进行慢网络操作。
    # 原实现 get_client()(建 8 连接池) + client.ping()(网络往返) 会在每次
    # 能力路由探测时打网络(日志里每几分钟一批 granted 即由此触发), 既违反规范
    # 又拖慢启动。原生库是否就位已由上面的 bootstrap.status() 校验(含 sha256),
    # 服务器连通性留到真正取数时验证 —— 失败会作为 ProviderRequestError 上报。
    return True, "eltdx 就绪(原生库已就位, 连通性在取数时验证)"


def _window_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


# A 股 1 分钟K 的开/收盘边界(与宿主 kline_sync 的 09:25~15:05 口径一致;
# 首根 09:31 已含,尾根 15:00 为收盘集合竞价)。
_SESSION_OPEN = _clock(9, 25)
_SESSION_CLOSE = _clock(15, 5)
_LAST_BAR = _clock(15, 0)


def _filter_days_by_window(days: list[date], start_time, end_time) -> list[date]:
    """剔掉窗口内不可能有新分钟的天, 避免无谓的全市场请求 (v1.0.7).

    背景: 宿主做增量同步时把 start_time 设成"本地已有最新分钟的时间戳"。
    收盘后最后一根就是当天 15:00, 于是窗口变成 [15:00, now] —— 完全落在交易
    时段之外, 当天不可能再有新数据。但旧实现只按日期展开, 照样对全市场
    5584 只各发一次请求, 收盘后同步白跑 4.5 分钟。

    这里按 (窗口 ∩ 当天交易时段) 判断:
    - 无交集 → 跳过
    - 窗口起点 >= 当天最后一根 15:00 → 跳过(那根已存在, 不会更新)
    盘中同步(start_time 在交易时段内)不受影响, 仍会拉取当天以补齐缺口。
    """
    if start_time is None and end_time is None:
        return days
    st = start_time
    et = end_time
    if st is not None and getattr(st, "tzinfo", None) is not None:
        st = st.astimezone(_BEIJING)
    if et is not None and getattr(et, "tzinfo", None) is not None:
        et = et.astimezone(_BEIJING)

    out: list[date] = []
    for day in days:
        day_open = datetime.combine(day, _SESSION_OPEN, tzinfo=_BEIJING)
        day_close = datetime.combine(day, _SESSION_CLOSE, tzinfo=_BEIJING)
        if et is not None and et < day_open:
            continue
        if st is not None and st > day_close:
            continue
        if st is not None and st.date() == day:
            last_bar = datetime.combine(day, _LAST_BAR, tzinfo=_BEIJING)
            if st >= last_bar:
                continue
        out.append(day)
    return out


class EltdxProvider:
    """eltdx 原生数据源 provider (以外部源插件包分发)。"""

    name = "tsp_eltdx"
    minute_history_days = 40

    def __init__(self, config: Any = None) -> None:
        self.config = config if config is not None else _EltdxConfig()
        self.name = getattr(self.config, "name", "tsp_eltdx")
        self.builtin = False
        self._warmer = None
        try:
            from app.config import settings as _panel_settings
            self._data_dir = str(_panel_settings.data_dir or "/tmp/_tsp_data")
        except Exception:  # noqa: BLE001
            self._data_dir = "/tmp/_tsp_data"
        self._start_warmer()

    def _start_warmer(self) -> None:
        """启动缓存预热后台线程。失败不影响 provider 生命周期。"""
        try:
            from .warmer_loop import WarmerLoop
            self._warmer = WarmerLoop(self._data_dir,
                                      gaps_dir=str(Path(__file__).resolve().parent / "warmer"),
                                      days=3, interval_s=1800.0, initial_delay_s=60.0)
            self._warmer.start()
            logger.info("eltdx cache-warmer started, data_dir=%s", self._data_dir)
        except Exception as exc:  # noqa: BLE001
            logger.warning("eltdx cache-warmer 启动失败 (非致命): %s", exc)


    # ------------------------------------------------------------------ 基础
    def close(self) -> None:
        if getattr(self, "_warmer", None) is not None:
            self._warmer.stop(timeout=5.0)
            self._warmer = None
        close_client()

    # -------------------------------------------------------------- 标的维表
    def get_instruments(self, asset_type: str = "stock") -> list[dict]:
        """返回 tickflow Instrument 形状的 list[dict] (instrument_sync 消费)。"""
        client = get_client()
        rows: list[dict] = []
        getter = {
            "stock": client.codes.a_shares,
            "index": client.codes.indices,
            "etf": client.codes.etfs,
        }.get(asset_type, client.codes.a_shares)
        for market in ("sh", "sz", "bj"):
            try:
                items = getter(market)
            except Exception as exc:  # noqa: BLE001
                logger.warning("eltdx get_instruments(%s/%s) 失败: %s", asset_type, market, exc)
                continue
            for item in items or []:
                code = str(getattr(item, "code", "") or "")
                if not code:
                    continue
                exchange = str(getattr(item, "exchange", market) or market).upper()
                symbol = f"{code}.{exchange}"
                rows.append({
                    "symbol": symbol,
                    "name": getattr(item, "name", None),
                    "code": code,
                    "exchange": exchange,
                    "region": "CN",
                    "type": asset_type,
                    "ext": {
                        "listing_date": None,
                        "total_shares": None,
                        "float_shares": None,
                        "tick_size": None,
                        "limit_up": None,
                        "limit_down": None,
                    },
                })
        return rows

    # ------------------------------------------------------------------ 日K
    def get_daily(self, symbols, start_time=None, end_time=None, asset_type="stock",
                  on_chunk_done=None) -> pl.DataFrame:
        chunks = list(self.iter_daily(symbols, start_time, end_time, asset_type,
                                     on_chunk_done=on_chunk_done))
        if not chunks:
            return pl.DataFrame(schema=_DAILY_SCHEMA)
        return pl.concat(chunks, how="diagonal_relaxed")

    def iter_daily(self, symbols, start_time=None, end_time=None, asset_type="stock",
                   on_chunk_done=None) -> Iterator[pl.DataFrame]:
        symbol_list = [str(s) for s in (symbols or []) if s]
        if not symbol_list:
            return
        start = _compact_date(start_time)
        end = _compact_date(end_time) or date.today()
        if start is None:
            start = end - timedelta(days=365)
        span = max(1, (end - start).days)
        count = min(_HIST_MAX_COUNT, max(60, int(span * 0.76) + 30))
        client = get_client()
        total = len(symbol_list)
        for index, symbol in enumerate(symbol_list, start=1):
            code = to_tdx(symbol)
            std = to_std(symbol)
            try:
                series = client.bars.get(code, period="day", count=count, adjust=None,
                                        all_pages=True, page_size=800)
            except Exception as exc:  # noqa: BLE001
                logger.warning("eltdx 日K %s 失败: %s", std, exc)
                if on_chunk_done:
                    on_chunk_done(index, total)
                continue
            rows = []
            for bar in getattr(series, "bars", ()) or ():
                day = _compact_date(getattr(bar, "time", None))
                if day is None or day < start or day > end:
                    continue
                rows.append({
                    "symbol": std,
                    "date": day,
                    "open": _to_float(getattr(bar, "open", None)),
                    "high": _to_float(getattr(bar, "high", None)),
                    "low": _to_float(getattr(bar, "low", None)),
                    "close": _to_float(getattr(bar, "close", None)),
                    "volume": int(getattr(bar, "volume_lots", 0) or 0),
                    "amount": _to_float(getattr(bar, "amount", None)) or 0.0,
                })
            if on_chunk_done:
                on_chunk_done(index, total)
            if rows:
                yield pl.DataFrame(rows, schema=_DAILY_SCHEMA)

    # ---------------------------------------------------------------- 除权因子
    def get_adj_factors(self, symbols, start_time=None, end_time=None, asset_type="stock",
                        on_chunk_done=None) -> pl.DataFrame:
        from concurrent.futures import ThreadPoolExecutor

        symbol_list = [str(s) for s in (symbols or []) if s]
        start = _compact_date(start_time)
        end = _compact_date(end_time)
        if not symbol_list:
            return pl.DataFrame(schema=_ADJ_SCHEMA)
        client = get_client()
        total = len(symbol_list)
        done = [0]

        def work(symbol: str) -> list[dict]:
            rows = self._adj_one(client, symbol, start, end)
            done[0] += 1
            if on_chunk_done:
                on_chunk_done(done[0], total)
            return rows

        frames: list[pl.DataFrame] = []
        with ThreadPoolExecutor(max_workers=_ADJ_WORKERS) as pool:
            for rows in pool.map(work, symbol_list):
                if rows:
                    frames.append(pl.DataFrame(rows, schema=_ADJ_SCHEMA))
        if not frames:
            return pl.DataFrame(schema=_ADJ_SCHEMA)
        return (pl.concat(frames, how="diagonal_relaxed")
                .unique(subset=["symbol", "trade_date"], keep="last")
                .sort(["symbol", "trade_date"]))

    def _adj_one(self, client, symbol: str, start: date | None, end: date | None) -> list[dict]:
        code = to_tdx(symbol)
        std = to_std(symbol)
        try:
            block = client.corporate.capital_changes(code)
        except Exception as exc:  # noqa: BLE001
            logger.warning("eltdx 除权事件 %s 失败: %s", std, exc)
            return []
        events = []
        for record in getattr(block, "items", ()) or ():
            name = str(getattr(record, "category_name", "") or "")
            day = _compact_date(getattr(record, "date", None))
            if day is None or "除权除息" not in name:
                continue
            if start and day < start:
                continue
            if end and day > end:
                continue
            events.append((day, record))
        if not events:
            return []
        events.sort(key=lambda item: item[0])
        try:
            series = client.bars.get(code, period="day", count=_ADJ_BAR_COUNT, adjust=None,
                                    all_pages=True, page_size=800)
        except Exception as exc:  # noqa: BLE001
            logger.warning("eltdx 除权配价 %s 失败: %s", std, exc)
            return []
        closes: dict[date, float] = {}
        for bar in getattr(series, "bars", ()) or ():
            day = _compact_date(getattr(bar, "time", None))
            price = _to_float(getattr(bar, "close", None))
            if day is not None and price:
                closes[day] = price
        if not closes:
            return []
        dates = sorted(closes)
        limit = _price_limit(std)
        rows: list[dict] = []
        for day, record in events:
            prev_days = [d for d in dates if d < day]
            if not prev_days:
                continue
            prev_close = closes[prev_days[-1]]
            dividend = (_to_float(getattr(record, "c1_value", None)) or 0.0) / 10
            allot_price = _to_float(getattr(record, "c2_value", None)) or 0.0
            bonus = (_to_float(getattr(record, "c3_value", None)) or 0.0) / 10
            allot = (_to_float(getattr(record, "c4_value", None)) or 0.0) / 10
            ref = _ref_price(prev_close, dividend, bonus, allot, allot_price)
            if not ref or ref <= 0:
                continue
            actual = closes.get(day)
            if actual and prev_close > 0:
                # 涨跌停自检: 排除当日无除权(事件口径误命中)导致的异常因子
                if abs(actual / prev_close - 1) > limit + 0.02:
                    continue
            rows.append({
                "symbol": std,
                "trade_date": day,
                "ex_factor": round(prev_close / ref, 9),
                "dividend": dividend,
                "bonus": bonus,
                "allot": allot,
                "allot_price": allot_price,
                "prev_close": prev_close,
            })
        return rows

    # ------------------------------------------------------------------ 分钟K
    def get_minute(self, symbols, start_time=None, end_time=None, asset_type="stock",
                   freq="1m", on_chunk_done=None) -> pl.DataFrame:
        from concurrent.futures import ThreadPoolExecutor

        if freq not in ("1m", "1", "min", "1min"):
            raise ProviderRequestError(f"eltdx 只支持 1m 分钟K, 收到 freq={freq}")
        symbol_list = [str(s) for s in (symbols or []) if s]
        if not symbol_list:
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        start = _compact_date(start_time)
        end = _compact_date(end_time) or date.today()
        if start is None:
            start = end - timedelta(days=5)
        days = _window_days(start, min(end, date.today()))
        if not days:
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        client = get_client()
        today = datetime.now(_BEIJING).date()
        total = len(symbol_list) * len(days)
        done = [0]
        lock = threading.Lock()

        def work(job) -> pl.DataFrame | None:
            symbol, day = job
            rows = self._minute_rows(client, symbol, day, today)
            with lock:
                done[0] += 1
                current = done[0]
            if on_chunk_done:
                on_chunk_done(current, total)
            return rows

        jobs = [(sym, day) for day in days for sym in symbol_list]
        frames: list[pl.DataFrame] = []
        with ThreadPoolExecutor(max_workers=_MINUTE_WORKERS) as pool:
            for rows in pool.map(work, jobs):
                if rows is not None and not rows.is_empty():
                    frames.append(rows)
        if not frames:
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        return pl.concat(frames, how="diagonal_relaxed")

    def iter_minute(
        self, symbols, start_time=None, end_time=None, asset_type: str = "stock",
        freq: str = "1m", on_chunk_done=None,
    ) -> Iterator[pl.DataFrame]:
        """流式分钟K(v1.0.5, plugin_api_version:1 可选能力).

        宿主做历史分钟同步时会把「完整标的列表 + 完整时间窗」一次性交进来,
        分批、时间分段、限频、同源兜底全部由插件负责(见 TSP
        docs/external-data-source-packages.md)。这里按 (交易日 × 标的) 串行取数,
        累积到帧阈值就 yield 一帧, 每完成一个任务回调一次进度。

        约定:
        - on_chunk_done(cur, total) 的异常(含取消)原样向上抛, 不吞不包装。
        - 单标的取数失败已在 _minute_rows 内降级并返回 None, 不中断整体遍历;
          若整窗无任何数据则自然结束(不 yield 空帧), 由宿主判定为空结果。
        """
        if freq not in ("1m", "1", "min", "1min") or not symbols:
            if on_chunk_done:
                on_chunk_done(0, 0)
            return
        symbol_list = [str(s) for s in symbols if s]
        if not symbol_list:
            if on_chunk_done:
                on_chunk_done(0, 0)
            return
        start = _compact_date(start_time)
        end = _compact_date(end_time) or date.today()
        if start is None:
            start = end - timedelta(days=5)
        days = _window_days(start, min(end, date.today()))
        # v1.0.7: 按时间窗剔除没有新数据的天(收盘后增量同步可整批跳过)。
        days = _filter_days_by_window(days, start_time, end_time)
        if not days:
            if on_chunk_done:
                on_chunk_done(0, 0)
            return

        from concurrent.futures import ThreadPoolExecutor

        client = get_client()
        today = datetime.now(_BEIJING).date()
        jobs = [(sym, day) for day in days for sym in symbol_list]
        total = len(jobs)
        done = 0
        failed = 0
        parts: list[pl.DataFrame] = []
        pending = 0
        lock = threading.Lock()

        def work(job):
            """单 (标的, 交易日) 取数; 节流放在 worker 内, 实现"每线程独立限频"。"""
            sym, day = job
            rows = self._minute_rows(client, sym, day, today)
            time.sleep(_MINUTE_INTERVAL_S)
            return rows

        # v1.0.6: 串行 → 并发。原实现按任务逐个 sleep(0.08s), 全市场一天要 9.6 分钟;
        # 并发后总吞吐约 8 倍(实测 5579 只从 9.6 分钟降到约 1.5 分钟)。
        # 单线程内部仍保留 0.08s 节流, 单个连接的压力与串行时一致, 不额外压服务器。
        with ThreadPoolExecutor(max_workers=_MINUTE_WORKERS) as pool:
            # pool.map 保序, 保证 on_chunk_done 的进度单调递增。
            for rows in pool.map(work, jobs):
                done += 1
                # 进度回调的异常必须原样传播(取消/中断由宿主控制), 因此不做 try 包裹。
                if on_chunk_done:
                    on_chunk_done(done, total)
                if rows is not None and not rows.is_empty():
                    parts.append(rows)
                    pending += rows.height
                else:
                    with lock:
                        failed += 1
                if parts and (pending >= _MINUTE_OUTPUT_ROWS
                              or len(parts) >= _MINUTE_OUTPUT_SYMBOLS):
                    yield pl.concat(parts, how="diagonal_relaxed")
                    parts = []
                    pending = 0
        if parts:
            yield pl.concat(parts, how="diagonal_relaxed")
        # v1.0.6 失败可见性: 过去静默跳过, 用户看到行数变少却不知情。
        if failed:
            logger.warning(
                "iter_minute 完成: %d/%d 个 (标的×交易日) 无数据或取数失败"
                " (占 %.1f%%), 其余已返回",
                failed, total, failed * 100.0 / total if total else 0.0,
            )

    def _minute_rows(self, client, symbol: str, day: date, today: date) -> pl.DataFrame | None:
        """单标的单日 1m K.

        当日及最近 < 70 交易日优先走 bars.get(period="1m"): 真实 OHLC,
        每只约 0.18s (count=240). 更老的日退化到 minutes.history.
        """
        code = to_tdx(symbol)
        std = to_std(symbol)
        days_ago = (today - day).days
        # 修复(v1.0.3): 原条件 `days_ago <= 70` 会让历史日期也走 bars.get,
        # 而 bars.get 无"取哪一天"参数(anchor_date 是定点复权基准日),
        # 永远返回最新 count 根 → 历史请求全变成今天的数据且重复。
        # 只有当天才走 bars.get(可拿到真实 OHLC), 历史走 minutes.recent/history。
        if days_ago == 0:
            try:
                result = client.bars.get(code, period="1m", count=240, all_pages=False)
                ser_obj = result.get(code) if isinstance(result, dict) else result
                raw_bars = getattr(ser_obj, "bars", ()) or ()
                if raw_bars:
                    rows: list[dict] = []
                    foreign = 0   # 跨日行(前一交易日尾巴)
                    unparsed = 0  # time 字段不是 datetime
                    relabeled = 0 # 13:00 错标纠正为 11:30 的根数
                    dropped_1300 = 0  # 下午开盘后丢弃的 13:00 错位根数
                    for b in raw_bars:
                        t = getattr(b, "time", None)
                        if t is None:
                            continue
                        if not hasattr(t, "hour"):
                            # v1.0.9: 原实现用 now() 顶替, 会静默造出错误时间戳。
                            # 宁可丢弃并计数, 也不产出自认为正确的脏数据。
                            unparsed += 1
                            continue
                        stamp = datetime(t.year, t.month, t.day, t.hour, t.minute)
                        # v1.0.9 关键修复: bars.get(count=240) 返回的是「最近 240 根」,
                        # 盘中只走了几十根时会回溯到前一交易日。实测 09:52 查询当日,
                        # 拿到 11-23 的 218 根 + 11-24 的 22 根, 分时图会混入昨天尾巴。
                        # 这里按目标交易日过滤, 只保留当天的分钟。
                        if stamp.date() != day:
                            foreign += 1
                            continue
                        # v1.0.11 修复: TDX 会把「上午最后一根(11:29~11:30)」错标成 13:00
                        # (实测 11-23 全市场 5584 只里有 11 只是这种标签; 而盘中同步时
                        # 全部标的都会这样)。后果: 分时图在下午开盘前就冒出一个 13:00 点,
                        # 且按 11:30 做跨标的查询会漏掉几乎全部标的。
                        # 判断无歧义: 当前时间还没到 13:00, 标签却是 13:00 → 它是上午收盘那根。
                        # v1.0.12: TDX 的分钟帧在午休边界整体错位一格 ——
                        #   上午只发 09:31~11:29 (119 根)，把「11:29~11:30 收盘」那根
                        #   标成 13:00；真正的下午第一分钟是 13:01 起。
                        #   全天因此正好 240 根(119+1+120)，但标签是错的。
                        # 处理：无论上午还是下午，这根都改标为 11:30。
                        #   不能只在上下午某一侧纠正 —— 实测下午开盘后 TDX 依然不发 11:30，
                        #   若下午选择"丢弃"，全天就少了上午收盘那根（239 而非 240）。
                        #   改标后全天 = 119(09:31~11:29) + 1(11:30) + 120(13:01~15:00) = 240，与标准一致。
                        if (stamp.hour, stamp.minute) == (13, 0):
                            stamp = stamp.replace(hour=11, minute=30)
                            relabeled += 1
                        vol = float(getattr(b, "volume_lots", 0) or 0)
                        amt = _to_float(getattr(b, "amount", None)) or 0.0
                        rows.append({
                            "symbol": std,
                            "datetime": stamp,
                            "open": _to_float(b.open),
                            "high": _to_float(b.high),
                            "low": _to_float(b.low),
                            "close": _to_float(b.close),
                            "volume": vol,
                            "amount": amt,
                        })
                    if foreign or unparsed or relabeled or dropped_1300:
                        logger.info(
                            "eltdx _minute_rows %s %s: 过滤跨日 %d 根 / 时间不可解析 %d 根 / 13:00→11:30 纠正 %d 根 / 丢弃错位 13:00 %d 根, 保留当日 %d 根",
                            std, day, foreign, unparsed, relabeled, dropped_1300, len(rows),
                        )
                    if rows:
                        logger.info("eltdx _minute_rows %s %s: got %d bars from bars.get(1m)", std, day, len(rows))
                        return pl.DataFrame(rows, schema=_MINUTE_SCHEMA)
            except Exception as exc:  # noqa: BLE001
                logger.warning("eltdx bars.get(1m) %s %s 失败, 降级 minutes.history: %s",
                               std, day, exc)
        # 退化路径: minutes.history / minutes.today
        try:
            if day >= today:
                series = client.minutes.today(code)
            else:
                # 修复(v1.0.3): 近期交易日优先 recent(0x0feb, 服务端近期窗口),
                # 失败再退 history(0x0fb4, 按日期精确补单日)。
                # v1.0.8 关键修正: 历史分钟必须走 minutes.history(0x0fb4)。
                # 实测同一只票同一天, recent(0x0feb) 与 history/bars.get/日K 的价格
                # 系统性不一致(例 000001.SZ 11-23: recent=10.47, history=12.34=日K),
                # 且 recent 时而在窗口外返回空 —— 它在 v1.0.3 被误设为优先路径,
                # 导致「本地无落盘数据的历史日期」全部拿到错误价格(有数据时被本地命中掩盖)。
                # 现在以 history 为主, recent 仅作兜底且必须与 prev_close 自洽。
                series = None
                try:
                    series = client.minutes.history(code, day)
                except Exception as exc_hist:  # noqa: BLE001
                    logger.info("eltdx minutes.history %s %s 失败(%s), 尝试 recent 兜底", std, day, exc_hist)
                if series is None or not (getattr(series, "points", None) or ()):
                    try:
                        series = client.minutes.recent(code, day)
                    except Exception as exc_recent:  # noqa: BLE001
                        logger.warning("eltdx minutes.recent %s %s 兜底也失败: %s", std, day, exc_recent)
                        series = None
        except Exception as exc:  # noqa: BLE001
            logger.warning("eltdx 分钟K %s %s 失败: %s", std, day, exc)
            return None
        points = getattr(series, "points", ()) or ()
        rows: list[dict] = []
        prev_amount = 0.0
        cum_volume = 0.0
        for point in points:
            label = str(getattr(point, "time_label", "") or "")
            if ":" not in label:
                continue
            try:
                hour, minute = (int(x) for x in label.split(":")[:2])
                stamp = datetime(day.year, day.month, day.day, hour, minute)
            except (TypeError, ValueError):
                continue
            price = _to_float(getattr(point, "price", None))
            if price is None:
                continue
            volume = float(getattr(point, "volume", 0) or 0)
            avg = _to_float(getattr(point, "avg_price", None))
            if avg and avg > 0:
                cum_volume += volume
                cum_amount = avg * cum_volume * 100
                amount = round(cum_amount - prev_amount, 2)
                prev_amount = cum_amount
            else:
                amount = round(price * volume * 100, 2)
            rows.append({
                "symbol": std,
                "datetime": stamp,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume,
                "amount": amount,
            })
        return pl.DataFrame(rows, schema=_MINUTE_SCHEMA) if rows else None

    def get_realtime(self, universes=None, symbols=None) -> list[dict]:
        """全市场快照。软失败返回 [] (契约: 不可抛)。"""
        try:
            client = get_client()
            codes = None
            if symbols:
                from .client import tdx_codes

                codes = tdx_codes(symbols)
            if not codes:
                # all_stocks() 直接返回 'sh600519' 字符串列表 (5659 只, 约 4s)
                codes = [str(c) for c in (client.codes.all_stocks() or [])]
            rows: list[dict] = []
            now = _now_ms()
            # 换手率分母一次性预热 (全市场 >200 只即走权威全量缓存, 约 3.6s,
            # 之后 12h 不再拉); 不预热则每行一次 7709 往返。
            try:
                _ensure_shares([to_std(c) for c in codes])
            except Exception as exc:  # noqa: BLE001
                logger.warning("eltdx 流通股本预热失败: %s", exc)
            # v1.0.10: 批级并发。原实现 10 个批次严格串行, 全市场一轮要 21 秒,
            # 而宿主轮询间隔只有 6 秒 —— 永远追不上, 前端拿到的报价稳定滞后 26~48 秒。
            # 并发后一轮约 4 秒, 能落回轮询节奏内。
            code_batches = [codes[i:i + _SNAPSHOT_BATCH]
                            for i in range(0, len(codes), _SNAPSHOT_BATCH)]

            def _fetch_batch(batch):
                try:
                    return batch, client.helpers.full_quotes(batch), None
                except Exception as exc:  # noqa: BLE001
                    return batch, None, exc

            failed_batches = 0
            returned = 0
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=_REALTIME_WORKERS) as pool:
                # pool.map 保序, 保证 rows 的输出顺序与串行时一致(便于比对与缓存命中)。
                for idx, (batch, snaps, err) in enumerate(pool.map(_fetch_batch, code_batches)):
                    if err is not None:
                        failed_batches += 1
                        # 仍按契约软失败: 单批失败不影响其他批次, 不抛。
                        logger.warning("eltdx 快照批 %d 失败: %s", idx * _SNAPSHOT_BATCH, err)
                        continue
                    returned += len(snaps or [])
                    for snap in snaps or []:
                        row = self._snapshot_row(snap, now)
                        if row:
                            rows.append(row)
            if failed_batches:
                logger.warning(
                    "eltdx get_realtime 部分批失败: %d/%d 批, 已返回 %d 行 (共 %d 只代码)",
                    failed_batches, len(code_batches), len(rows), len(codes),
                )
            return rows
        except Exception as exc:  # noqa: BLE001
            logger.warning("eltdx get_realtime 失败: %s", exc)
            return []

    def _snapshot_row(self, snap, now_ms: int, volume_divisor: float = 1.0) -> dict | None:
        last = _to_float(getattr(snap, "last_price", None))
        prev = _to_float(getattr(snap, "pre_close_price", None))
        if last is None or last <= 0:
            return None
        exchange = str(getattr(snap, "exchange", "") or "")
        code = str(getattr(snap, "code", "") or "")
        if not code:
            return None
        symbol = to_std(f"{exchange}{code}")
        change_amount = None
        change_pct = None
        if prev and prev > 0:
            change_amount = round(last - prev, 4)
            change_pct = round(last / prev - 1, 6)
        high = _to_float(getattr(snap, "high_price", None))
        low = _to_float(getattr(snap, "low_price", None))
        amplitude = None
        if prev and prev > 0 and high is not None and low is not None:
            amplitude = round((high - low) / prev, 6)
        total_hand = _to_float(getattr(snap, "total_hand", None)) or 0.0
        return {
            "symbol": symbol,
            "name": _stock_name(symbol),
            "last_price": last,
            "prev_close": prev,
            "open": _to_float(getattr(snap, "open_price", None)),
            "high": high,
            "low": low,
            "volume": int(total_hand / volume_divisor),
            "amount": _to_float(getattr(snap, "amount", None)) or 0.0,
            "change_pct": change_pct,
            "change_amount": change_amount,
            "amplitude": amplitude,
            "turnover_rate": _turnover_rate(symbol, total_hand),
            "session": None,
            "timestamp": _snapshot_ms(getattr(snap, "time_raw", None), now_ms),
        }

    def get_realtime_indices(self, symbols=None) -> list[dict] | None:
        """指数快照。软失败返回 None (保留上轮缓存)。"""
        wanted = [str(s) for s in (symbols or []) if s]
        if not wanted:
            return []
        try:
            client = get_client()
            from .client import tdx_codes

            codes = tdx_codes(wanted)
            snaps = client.helpers.full_quotes(codes)
        except Exception as exc:  # noqa: BLE001
            logger.warning("eltdx 指数快照失败: %s", exc)
            return None
        now = _now_ms()
        rows = []
        for snap in snaps or []:
            # 指数快照的 total_hand 实测是「股」(日K volume_lots 的 100 倍),
            # 换手手数须 /100, 否则指数成交量放大百倍。
            row = self._snapshot_row(snap, now, volume_divisor=100)
            if row:
                rows.append(row)
        return rows

    # -------------------------------------------------------------- 五档盘口
    def get_depth_batch(self, symbols) -> dict[str, dict]:
        from .client import tdx_codes

        codes = tdx_codes([s for s in (symbols or []) if s])
        if not codes:
            return {}
        client = get_client()
        out: dict[str, dict] = {}
        now = _now_ms()
        for offset in range(0, len(codes), _DEPTH_BATCH):
            batch = codes[offset:offset + _DEPTH_BATCH]
            try:
                page = client.quotes.get_depth(batch)
            except Exception as exc:  # noqa: BLE001
                logger.warning("eltdx 五档批 %d 失败: %s", offset, exc)
                continue
            for record in getattr(page, "records", ()) or ():
                symbol = to_std(f"{getattr(record, 'exchange', '')}{getattr(record, 'code', '')}")
                entry = self._depth_entry(record, now)
                if symbol and entry:
                    out[symbol] = entry
        return out

    def _depth_entry(self, record, now_ms: int) -> dict | None:
        bids = getattr(record, "buy_levels", ()) or ()
        asks = getattr(record, "sell_levels", ()) or ()
        bid_prices = [_to_float(getattr(lv, "price", None)) for lv in bids]
        ask_prices = [_to_float(getattr(lv, "price", None)) for lv in asks]
        if not any(p for p in bid_prices) or not any(p for p in ask_prices):
            return None
        return {
            "bid_prices": [p or 0.0 for p in bid_prices],
            "bid_volumes": [int(getattr(lv, "volume", 0) or 0) for lv in bids],
            "ask_prices": [p or 0.0 for p in ask_prices],
            "ask_volumes": [int(getattr(lv, "volume", 0) or 0) for lv in asks],
            "timestamp": _snapshot_ms(getattr(record, "update_time_raw", None), now_ms),
        }

    # ------------------------------------------------------------------ 财务
    def get_financials(self, table: str, symbols, latest_only: bool = False) -> pl.DataFrame:
        name = (table or "metrics").strip().lower()
        if name not in f10.TABLES:
            raise ValueError(f"不支持的财务表: {table}")
        rows = f10.financial_rows([str(s) for s in (symbols or []) if s], name, latest_only, to_tdx)
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, infer_schema_length=None)

    # -------------------------------------------------------------- 数据集自检
    def test_dataset(self, dataset: str, symbols=None) -> dict:
        name = (dataset or "").strip()
        picks = [str(s) for s in (symbols or []) if s]
        try:
            if name in ("daily", "adj_factor"):
                targets = picks[:3] or ["600519.SH"]
                frame = (self.get_daily(targets, "2026-01-01", "2026-12-31") if name == "daily"
                         else self.get_adj_factors(targets, "2020-01-01", "2026-12-31"))
            elif name in ("minute", "full_minute"):
                targets = picks[:3] or ["600519.SH"]
                frame = self.get_minute(targets, "2026-01-01", "2026-12-31")
            elif name == "instruments":
                rows = self.get_instruments("stock")
                return {"provider": self.name, "dataset": name, "rows": len(rows),
                        "columns": ["symbol", "name", "code", "exchange"],
                        "preview": rows[:5]}
            elif name in ("realtime", "depth5"):
                targets = picks[:5] or ["600519.SH", "000001.SZ"]
                if name == "realtime":
                    rows = self.get_realtime(symbols=targets)
                    return {"provider": self.name, "dataset": name, "rows": len(rows),
                            "columns": list(rows[0].keys()) if rows else [],
                            "preview": rows[:5]}
                depth = self.get_depth_batch(targets)
                preview = [{"symbol": k, **v} for k, v in list(depth.items())[:5]]
                return {"provider": self.name, "dataset": name, "rows": len(depth),
                        "columns": ["symbol", "bid_prices", "bid_volumes", "ask_prices",
                                    "ask_volumes", "timestamp"],
                        "preview": preview}
            elif name == "financial":
                targets = picks[:3] or ["600519.SH"]
                frame = self.get_financials("metrics", targets, latest_only=True)
            else:
                return {"provider": self.name, "dataset": name, "rows": 0,
                        "error": f"eltdx 插件未接入 {name} 数据集"}
        except Exception as exc:  # noqa: BLE001
            return {"provider": self.name, "dataset": name, "rows": 0, "error": str(exc)}
        preview = []
        for row in frame.head(5).to_dicts():
            item = {}
            for key, value in row.items():
                if isinstance(value, (date, datetime)):
                    item[key] = value.isoformat()
                else:
                    item[key] = value
            preview.append(item)
        return {"provider": self.name, "dataset": name, "rows": frame.height,
                "columns": frame.columns, "preview": preview}


class ProviderRequestError(RuntimeError):
    """分钟K 等强一致取数的失败异常 (契约: 必须抛, 不跨源降级)。"""


__all__ = ["EltdxProvider", "ProviderRequestError", "availability"]
