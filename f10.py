"""F10(7615 TQLEX) -> tick-stock-panel 财务表。

面板的「财务数据」是**一个 dataset 覆盖五张表**，请求体里带 table 参数
（backend/app/data_providers/custom/provider.py:386 get_financials）。
本模块把 eltdx 的四类 F10 接口拼成那五张表：

  zcfzb 资产负债表  -> balance_sheet
  lyb   利润表      -> income
  xjllb 现金流量表  -> cash_flow
  zyzb  主要指标    -> metrics 的每股类字段(单季口径, 按年累加)
  cwbg  财报披露    -> announce_date(取巨潮公告链接里的日期)
  gbjg  股本结构    -> shares

口径要点（与面板 fuyao 口径对齐, 见 frontend/src/pages/settings/
DataSourceEditor.tsx 的 FINANCIAL_ALL_FIELDS）：
- 三大报表是「年初至今累计」；zyzb 是「单季」, 每股指标须按年累加。
- 金额单位一率是元；比例字段是百分点(12.3 表示 12.3%)。
- 银行/券商/保险(nhytype 1/2/3)与通用企业(0)的 T 编号**不同义**
  (T039 通用=资产总计, 银行=固定资产), 按 nhytype 选模板, 不跨模板猜。
- announce_date 取公告链接里的日期并取同报告期**最早**一条(最早可用,
  不会造成前视)；取不到就留空, 由面板按 period_end 兜底, 不伪造。
"""
from __future__ import annotations

import gzip
import http.client
import json
import math
import os
import queue
import re
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlencode

CACHE_DIR = Path(__file__).resolve().parent / "cache" / "f10"
CACHE_TTL = 12 * 3600
MAX_WORKERS = 8
# latest=True 时返回最近多少期（而非仅最新一期）。
# 面板 _sync_history_table_for_symbols 对「已有历史的标的」只发 latest=True，
# 若这里只回 1 期，metrics 这类此前只落过单期的表就永远补不出历史（回测全是未来函数）。
LATEST_PERIODS = 200
TABLES = ("metrics", "income", "balance_sheet", "cash_flow", "shares")

# --- 7615 TQLEX 长连接池 ---------------------------------------------------
#
# 为什么不用 eltdx 自带的 F10Client: 它每次请求都新建 TCP 连接(urlopen),
# 实测单请求 0.26s 中 ~0.2s 花在握手, 且服务端对「每请求新连接」的短连
# 模式有约 20% 概率直接 RemoteDisconnected。改成固定 IP 的长连接复用后,
# 同一台机器同样的请求序列 40/40 成功、4.5 req/s（原来 0.7 req/s）。
#
# 解析仍用 eltdx 的 parse_tqlex_response, 只换传输层。
_TQLEX_HOST = "static.tdx.com.cn"
_TQLEX_PORT = 7615
_TQLEX_PATH = "/TQLEX"
_ENTRY_FINANCE = "CWServ.tdxf10_gg_cwfx"
_ENTRY_SHARES = "CWServ.tdxf10_gg_gbjg"

_POOL: queue.LifoQueue = queue.LifoQueue()
_POOL_LOCK = threading.Lock()
_POOL_READY = [False]


# 服务端对空闲连接有自己的回收节奏（实测十几秒不使用就会被掐）。
# 复用一个已被对端关掉的 socket 会立刻 RemoteDisconnected，所以
#   a) 空闲超过 IDLE_RESET 秒的连接先主动重建；
#   b) 传输层再加一层「换新连接重试」，把偶发断连消化在这一层，
#      让上层的财务语义重试只处理真正的数据缺失。
_IDLE_RESET = 8.0


class _Conn:
    """一条到 7615 的持久连接；出错就丢弃重连，由调用方重试。"""

    def __init__(self) -> None:
        self.raw: http.client.HTTPConnection | None = None
        self.last_used = 0.0
        self._ip = socket.getaddrinfo(_TQLEX_HOST, _TQLEX_PORT,
                                      socket.AF_INET, socket.SOCK_STREAM)[0][4][0]

    def close(self) -> None:
        if self.raw is not None:
            try:
                self.raw.close()
            except Exception:  # noqa: BLE001
                pass
            self.raw = None

    def _ensure(self) -> None:
        if self.raw is not None and time.monotonic() - self.last_used > _IDLE_RESET:
            self.close()
        if self.raw is None:
            self.raw = http.client.HTTPConnection(self._ip, _TQLEX_PORT, timeout=20)

    def post(self, entry: str, params: list) -> bytes:
        self._ensure()
        body = json.dumps({"Params": params}, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8")
        url = f"{_TQLEX_PATH}?{urlencode({'Entry': entry})}"
        try:
            self.raw.request("POST", url, body=body, headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "eltdx/1.0",
                "Host": _TQLEX_HOST,
            })
            resp = self.raw.getresponse()
            data = resp.read()
        finally:
            self.last_used = time.monotonic()
        if resp.status != 200:
            raise RuntimeError(f"TQLEX HTTP {resp.status} for {entry}")
        return data


def _pool_init() -> None:
    with _POOL_LOCK:
        if _POOL_READY[0]:
            return
        for _ in range(MAX_WORKERS):
            _POOL.put(_Conn())
        _POOL_READY[0] = True


def _tqlex(entry: str, params: list):
    """从连接池取一条长连接发一次 TQLEX 请求，返回解析后的 F10Response。"""
    from eltdx.f10.client import parse_tqlex_response

    _pool_init()
    last: Exception | None = None
    for attempt in range(3):
        conn = _POOL.get()
        try:
            raw_bytes = conn.post(entry, params)
        except Exception as exc:  # noqa: BLE001  传输层断连, 换新连接重试
            conn.close()
            _POOL.put(_Conn())
            last = exc
            time.sleep(0.15 * (attempt + 1))
            continue
        _POOL.put(conn)
        raw = json.loads(raw_bytes.decode("utf-8-sig"))
        return parse_tqlex_response(entry, {"Params": params}, raw)
    raise last  # type: ignore[misc]


def _code6(code: str) -> str:
    text = str(code).strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if text.startswith(prefix):
            return text[2:]
    return text


def _finance_report(code: str, report_type: str):
    return _tqlex(_ENTRY_FINANCE, [_code6(code), report_type, ""])


def _capital_structure(code: str):
    return _tqlex(_ENTRY_SHARES, [_code6(code), "gbjg"])

# ---------------------------------------------------------------------------
# 字段模板: 同一 canonical 列在四个行业模板里的 T 编号
# ---------------------------------------------------------------------------
_BS_GEN = {
    "total_assets": "T039",
    "total_current_assets": "T020",
    "total_non_current_assets": "T038",
    "cash_and_equivalents": "T007",
    "accounts_receivable": "T120",
    "inventory": "T016",
    "fixed_assets": "T026",
    "intangible_assets": "T032",
    "goodwill": "T034",
    "total_liabilities": "T062",
    "total_current_liabilities": "T053",
    "total_non_current_liabilities": "T061",
    "short_term_borrowing": "T040",
    "long_term_borrowing": "T054",
    "accounts_payable": "T043",
    "total_equity": "T071",
    "equity_attributable": "T077",
    "retained_earnings": "T067",
    "minority_interest": "T068",
    "other_equity_instruments": "T110",
}
_BS_BANK = {
    "total_assets": "T048",
    "cash_and_equivalents": "T010",
    "fixed_assets": "T039",
    "intangible_assets": "T041",
    "goodwill": "T109",
    "total_liabilities": "T083",
    "total_equity": "T093",
    "equity_attributable": "T099",
    "retained_earnings": "T089",
    "minority_interest": "T090",
    "other_equity_instruments": "T105",
}
_BS_SEC = dict(_BS_BANK)
_BS_SEC["cash_and_equivalents"] = "T008"
_BS_SEC["short_term_borrowing"] = "T051"
_BS_INS = dict(_BS_SEC)
_BS_INS["long_term_borrowing"] = "T078"

_IS_GEN = {
    "revenue": "T008",
    "operating_cost": "T009",
    "operating_profit": "T020",
    "selling_expense": "T011",
    "admin_expense": "T012",
    "rd_expense": "T061",
    "financial_expense": "T014",
    "non_operating_income": "T022",
    "non_operating_expense": "T023",
    "total_profit": "T026",
    "income_tax": "T027",
    "net_income": "T029",
    "net_income_attributable": "T030",
}
_IS_FIN = {
    "revenue": "T008",
    "operating_profit": "T042",
    "non_operating_income": "T044",
    "non_operating_expense": "T045",
    "total_profit": "T047",
    "income_tax": "T048",
    "net_income": "T050",
    "net_income_attributable": "T051",
}

_CF_GEN = {
    "net_operating_cash_flow": "T017",
    "net_investing_cash_flow": "T029",
    "net_financing_cash_flow": "T038",
    "capex": "T024",
    "net_cash_change": "T041",
}
_CF_FIN = {
    "net_operating_cash_flow": "T033",
    "net_investing_cash_flow": "T044",
    "net_financing_cash_flow": "T055",
    "capex": "T041",
    "net_cash_change": "T058",
}

_BS = {0: _BS_GEN, 1: _BS_BANK, 2: _BS_SEC, 3: _BS_INS}
_IS = {0: _IS_GEN, 1: _IS_FIN, 2: _IS_FIN, 3: _IS_FIN}
_CF = {0: _CF_GEN, 1: _CF_FIN, 2: _CF_FIN, 3: _CF_FIN}
_SHARE_FIELD = {0: "T063", 1: "T084", 2: "T084", 3: "T084"}

_DEDUCTED_FIELDS = ("cT010", "T010")


def _tpl(table_map: dict, nhytype: int) -> dict:
    return table_map.get(nhytype) or table_map[0]

# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------


def _num(v):
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return f if math.isfinite(f) else None
    try:
        f = float(str(v).strip())
    except Exception:
        return None
    return f if math.isfinite(f) else None


def _rows(resp) -> list[dict]:
    if resp is None or not resp.result_sets:
        return []
    for rs in resp.result_sets:
        if rs.rows:
            return list(rs.rows)
    return []


def _meta(resp) -> dict:
    if resp is None:
        return {}
    for rs in resp.result_sets:
        for row in rs.rows:
            if "nhytype" in row:
                return row
    return {}


_ANN_PATTERNS = (
    re.compile(r"/(\d{4}-\d{2}-\d{2})/"),
    re.compile(r"/(\d{4})/(\d{8})/"),
)


def announce_date_from_url(url: str | None) -> str | None:
    """从巨潮公告链接里取公告日。

    实测三种形态（见 docs 与真实数据）：
      .../finalpage/2026-10-24/xxx.PDF        -> 2026-10-24
      .../hsomarket/2026/20261030/xxx         -> 2026-10-30
      .../bse_onmarket/2026/2026-10-28/xxx    -> 2026-10-28
      .../bse_onmarket/bse/2026/20260814/xxx  -> 2026-08-14
    """
    text = str(url or "")
    m = _ANN_PATTERNS[0].search(text)
    if m:
        return m.group(1)
    m = _ANN_PATTERNS[1].search(text)
    if m:
        digital = m.group(2)
        return f"{digital[:4]}-{digital[4:6]}-{digital[6:]}"
    return None


def _prev_year_rq(rq: str) -> str:
    return f"{int(rq[:4]) - 1}{rq[4:]}"


def _avg(a, b):
    if a is None or b is None:
        return None
    return (a + b) / 2.0


def _pct(numerator, denominator):
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator * 100.0


def _yoy(cur, prev):
    if cur is None or prev in (None, 0):
        return None
    return (cur / prev - 1.0) * 100.0

# ---------------------------------------------------------------------------
# 单只标的派生
# ---------------------------------------------------------------------------


# 7615 网关实测会随机断连（RemoteDisconnected / ConnectionReset），不是限速
# 也不是并发过高：conc=2 与 conc=6 的失败率几乎一样（约 20%），所以必须
# 靠重试兜住，而不是靠降并发。三次重试后仍失败才认为该份数据取不到。
_REQUEST_RETRIES = 3
_RETRY_BACKOFF = (0.4, 1.2)


def _retry_call(fn):
    last = None
    for attempt in range(_REQUEST_RETRIES):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last = exc
            if attempt < len(_RETRY_BACKOFF):
                time.sleep(_RETRY_BACKOFF[attempt])
    raise last  # type: ignore[misc]


def _fetch(code: str):
    """拉取一只标的的六份 F10 数据；任何一份失败只影响该份。"""
    out = {}
    for key, fn in (
        ("zcfzb", lambda: _finance_report(code, "zcfzb")),
        ("lyb", lambda: _finance_report(code, "lyb")),
        ("xjllb", lambda: _finance_report(code, "xjllb")),
        ("zyzb", lambda: _finance_report(code, "zyzb")),
        ("cwbg", lambda: _finance_report(code, "cwbg")),
        ("gbjg", lambda: _capital_structure(code)),
    ):
        try:
            out[key] = _retry_call(fn)
        except Exception as exc:  # noqa: BLE001
            print(f"[financial] {code} {key} 失败(重试{_REQUEST_RETRIES}次): {exc}", flush=True)
            out[key] = None
    return out


def _cum_eps(zyzb_rows: list[dict]) -> tuple[dict, dict]:
    """zyzb 是单季口径, 按自然年累加得到「年初至今累计」每股指标。"""
    by_rq: dict[str, dict] = {}
    for row in zyzb_rows:
        rq = str(row.get("T002") or "")[:10]
        if not rq:
            continue
        by_rq[rq] = row
    eps: dict[str, float] = {}
    ocfps: dict[str, float] = {}
    for rq in by_rq:
        year = rq[:4]
        total_eps = 0.0
        total_ocf = 0.0
        seen_eps = False
        seen_ocf = False
        for other, row in by_rq.items():
            if other[:4] != year or other > rq:
                continue
            v = _num(row.get("mgsy"))
            if v is not None:
                total_eps += v
                seen_eps = True
            v = _num(row.get("mgxjll"))
            if v is not None:
                total_ocf += v
                seen_ocf = True
        if seen_eps:
            eps[rq] = total_eps
        if seen_ocf:
            ocfps[rq] = total_ocf
    return eps, ocfps


def derive(code: str, symbol: str) -> dict | None:
    raw = _fetch(code)
    z_rows = _rows(raw.get("zcfzb"))
    if not z_rows:
        return None
    try:
        nhytype = int(_meta(raw.get("zcfzb")).get("nhytype") or 0)
    except Exception:  # noqa: BLE001
        nhytype = 0
    if nhytype not in (0, 1, 2, 3):
        nhytype = 0
    bs_map = _tpl(_BS, nhytype)
    is_map = _tpl(_IS, nhytype)
    cf_map = _tpl(_CF, nhytype)
    share_field = _SHARE_FIELD[nhytype]

    bs_by = {str(r["rq"])[:10]: r for r in z_rows if r.get("rq")}
    is_by = {str(r["rq"])[:10]: r for r in _rows(raw.get("lyb")) if r.get("rq")}
    cf_by = {str(r["rq"])[:10]: r for r in _rows(raw.get("xjllb")) if r.get("rq")}

    ann: dict[str, str] = {}
    for row in _rows(raw.get("cwbg")):
        rq = str(row.get("rq") or "")[:10]
        day = announce_date_from_url(row.get("url"))
        if rq and day and (rq not in ann or day < ann[rq]):
            ann[rq] = day

    eps_cum, ocfps_cum = _cum_eps(_rows(raw.get("zyzb")))

    def bs_val(rq, key, mp=bs_map):
        row = bs_by.get(rq)
        if not row:
            return None
        return _num(row.get(mp.get(key, "")))

    balance_rows: list[dict] = []
    income_rows: list[dict] = []
    cash_rows: list[dict] = []
    metric_rows: list[dict] = []

    for rq in sorted(bs_by):
        base = {"symbol": symbol, "period_end": rq, "announce_date": ann.get(rq)}
        if bs_by[rq]:
            row = dict(base)
            for key, field in bs_map.items():
                row[key] = _num(bs_by[rq].get(field))
            balance_rows.append(row)

        if rq in is_by:
            row = dict(base)
            for key, field in is_map.items():
                row[key] = _num(is_by[rq].get(field))
            deducted = None
            for field in _DEDUCTED_FIELDS:
                deducted = _num(is_by[rq].get(field))
                if deducted is not None:
                    break
            row["net_income_deducted"] = deducted
            row["basic_eps"] = eps_cum.get(rq)
            income_rows.append(row)

        if rq in cf_by:
            row = dict(base)
            for key, field in cf_map.items():
                row[key] = _num(cf_by[rq].get(field))
            cash_rows.append(row)

        # ---- metrics ----
        prev_rq = _prev_year_rq(rq)
        prev_bs = bs_by.get(prev_rq)
        end_equity = bs_val(rq, "equity_attributable")
        end_assets = bs_val(rq, "total_assets")
        begin_equity = None
        begin_assets = None
        if prev_bs:
            begin_equity = _num(prev_bs.get(bs_map["equity_attributable"]))
            begin_assets = _num(prev_bs.get(bs_map["total_assets"]))
        total_shares = _num((bs_by.get(rq) or {}).get(share_field))

        revenue = _num((is_by.get(rq) or {}).get(is_map["revenue"])) if rq in is_by else None
        cost = _num((is_by.get(rq) or {}).get(is_map.get("operating_cost", ""))) if rq in is_by else None
        net_income = _num((is_by.get(rq) or {}).get(is_map["net_income"])) if rq in is_by else None
        net_attr = _num((is_by.get(rq) or {}).get(is_map["net_income_attributable"])) if rq in is_by else None
        ocf = _num((cf_by.get(rq) or {}).get(cf_map["net_operating_cash_flow"])) if rq in cf_by else None
        liabilities = bs_val(rq, "total_liabilities")
        inventory = bs_val(rq, "inventory")
        begin_inventory = None
        if prev_bs:
            begin_inventory = _num(prev_bs.get(bs_map.get("inventory", "")))

        prev_is = is_by.get(prev_rq)
        prev_revenue = None
        prev_net_income = None
        if prev_is:
            prev_revenue = _num(prev_is.get(is_map["revenue"]))
            prev_net_income = _num(prev_is.get(is_map["net_income"]))

        gross_margin = None
        if revenue not in (None, 0) and cost is not None:
            gross_margin = (revenue - cost) / revenue * 100.0

        # BPS 用「归母普通股权益」: 银行/保险的归母权益里含优先股/永续债
        # (其他权益工具), 不扣除会让 BPS 虚高、PB 因子系统性偏低。实测
        # 000001 归母权益 5567.01 亿 - 其他权益工具 800 亿 = 4767.01 亿,
        # / 194.06 亿股 = 24.56 元, 与 fuyao 口径一致。
        other_equity = bs_val(rq, "other_equity_instruments") or 0.0
        common_equity = None if end_equity is None else end_equity - other_equity

        metric_rows.append({
            **base,
            "eps_basic": eps_cum.get(rq),
            "bps": (common_equity / total_shares)
            if common_equity is not None and total_shares else None,
            "ocfps": ocfps_cum.get(rq),
            "roe": _pct(net_attr, _avg(begin_equity, end_equity)),
            "roa": _pct(net_income, _avg(begin_assets, end_assets)),
            "gross_margin": gross_margin,
            "net_margin": _pct(net_income, revenue),
            "debt_to_asset_ratio": _pct(liabilities, end_assets),
            "revenue_yoy": _yoy(revenue, prev_revenue),
            "net_income_yoy": _yoy(net_income, prev_net_income),
            "operating_cash_to_revenue": _pct(ocf, revenue),
            "inventory_turnover": (cost / _avg(begin_inventory, inventory))
            if cost is not None and _avg(begin_inventory, inventory) not in (None, 0) else None,
        })

    share_rows: list[dict] = []
    for row in _rows(raw.get("gbjg")):
        period_end = str(row.get("T002") or "")[:10]
        if not period_end:
            continue
        total_shares = _num(row.get("T003"))
        float_shares = _num(row.get("T004"))
        if float_shares is None:
            float_shares = _num(row.get("T019"))
        if total_shares is None and float_shares is None:
            continue
        share_rows.append({
            "symbol": symbol,
            "period_end": period_end,
            "announce_date": ann.get(period_end),
            "total_shares": total_shares,
            "float_shares": float_shares,
        })

    return {
        "metrics": metric_rows,
        "income": income_rows,
        "balance_sheet": balance_rows,
        "cash_flow": cash_rows,
        "shares": share_rows,
    }

# ---------------------------------------------------------------------------
# 磁盘缓存 + 批量入口
# ---------------------------------------------------------------------------


def _cache_path(code: str) -> Path:
    return CACHE_DIR / f"{code}.json.gz"


def _load_cache(code: str) -> dict | None:
    path = _cache_path(code)
    try:
        if time.time() - path.stat().st_mtime > CACHE_TTL:
            return None
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and "tables" in data:
            return data["tables"]
    except Exception:  # noqa: BLE001
        return None
    return None


def _save_cache(code: str, tables: dict) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        payload = {"code": code, "ts": int(time.time()), "tables": tables}
        tmp = _cache_path(code).with_suffix(".tmp")
        with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=5) as fh:
            json.dump(payload, fh, ensure_ascii=False, allow_nan=False)
        os.replace(tmp, _cache_path(code))
    except Exception as exc:  # noqa: BLE001
        print(f"[financial] {code} 缓存写入失败: {exc}", flush=True)


def symbol_tables(code: str, symbol: str, *, refresh: bool = False) -> dict | None:
    if not refresh:
        cached = _load_cache(code)
        if cached is not None:
            return cached
    tables = derive(code, symbol)
    if tables is None:
        return None
    _save_cache(code, tables)
    return tables


def financial_rows(symbols: list[str], table: str, latest: bool, to_tdx) -> list[dict]:
    """面板 get_financials 的桥接实现。

    单只失败只丢该只（面板侧任何异常都会整表落空, 不能让它发生）。
    """
    name = (table or "metrics").strip().lower()
    if name not in TABLES:
        raise ValueError(f"不支持的财务表: {table}")
    code_map = {to_tdx(s): s for s in symbols}
    codes = list(code_map)
    out: list[dict] = []
    failed: list[str] = []
    lock = threading.Lock()

    def work(code: str) -> None:
        symbol = code_map.get(code, code)
        try:
            tables = symbol_tables(code, symbol)
        except Exception as exc:  # noqa: BLE001
            print(f"[financial] {code} 派生失败: {exc}", flush=True)
            tables = None
        if not tables:
            with lock:
                failed.append(code)
            return
        rows = tables.get(name) or []
        if latest and rows:
            rows = sorted(rows, key=lambda r: r.get("period_end") or "")[-LATEST_PERIODS:]
        with lock:
            out.extend(rows)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        list(pool.map(work, codes))
    if failed:
        print(
            f"[financial] {name} 跳过 {len(failed)}/{len(codes)} 只（无 F10 数据或接口异常, "
            f"样例: {failed[:5]}）",
            flush=True,
        )
    return out

