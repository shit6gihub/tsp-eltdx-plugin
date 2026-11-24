"""盘前竞价数据模块 (eltdx 侧实现) — 供插件 compat 层调用。

面板现状 (为什么要这个模块)
--------------------------
frontend/src/custom/abnormal/Page.tsx:161 '竞价异动' tab 有两块:
  1. 盘前风向标 BenchmarkCard (:228) ← GET /api/market-recap/auction-benchmark
     (api/market_recap.py:57 → services/auction_benchmark.py) —— 硬编码 fuyao
     provider.short_term_benchmark()，fuyao 未配 Key 时整 tab 显示
     state=source_unavailable 引导态 (:186-201)。
  2. 全市场竞价扫描卡 (:207-226) —— 面板至今是「待采集任务启用」占位，
     因为没有任何数据源能一次给出全市场竞价快照。

本模块用 eltdx 同时解决这两块:
  * helpers.full_quotes(全市场代码) 一次拿 5659 条快照 (实测 12.5~13.5s)，
    其中 open_price / pre_close_price / open_amount_yuan 即竞价结果。
  * 按「竞价高开幅度」分桶，直接产出风向标所需的 raw_item 行。

数据口径
--------
auction_pct = (open_price / pre_close_price - 1) * 100   (百分数原值, 如 1.66)
  —— 与 panel AuctionBenchmarkItem.auction_pct 的百分比口径一致
  —— 注意 9:25 之后 open_price 才定版; 盘中/盘后取到的是「当日开盘价」, 同样正确
  —— 盘前 (9:15-9:25) 之前取到的是上一交易日快照, trade_date 会回退到上一交易日

分桶 (默认)
  * candidates  1.0% <= pct < 5.0%  —— 温和高开, 面板回测里这才是 alpha 来源
    (auction_benchmark.py 头注释: 名单当日 +0.54%, 但高开>=5% 子集当日 -1.97%)
  * chase_risk  pct >= 5.0%         —— 追高陷阱区, 前端标「追高风险」

输出
----
get_auction_benchmark() → 可直接喂给 services/auction_benchmark._base_payload 的
fuyao 兼容容器 {"date", "item":[{"thscode","ticker","name","auction_pct","tags"}]}；
面板拿到后会自己做 day0_oc / day0_pct / d1_pct 的本地收益 enrich 与落缓存。
另提供 get_auction_scan() 给「全市场竞价扫描」卡用 (含 open_amount 等明细)。

实测 (2026-11-17 02:5x, 快照为 2026-11-16 收盘态)
  latest_stock_list 5582 只 / 4.0s；all_stocks 5659 (含 B 股 sh900xxx) / 2.9~4.1s
  full_quotes(5659) 5659 条 / 12.5~13.5s
  live (last>0 & open>0 & prev>0) 5649；open∈[1%,5%) 195 只；open>=5% 少量
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

logger = logging.getLogger(__name__)

_CN_TZ = timezone(timedelta(hours=8))
_AUCTION_READY = (9, 25)  # 9:25 竞价撮合完成, open_price 定版

_EX_MAP = {"sh": ".SH", "sz": ".SZ", "bj": ".BJ"}

DEFAULT_MIN_PCT = 1.0
DEFAULT_MAX_PCT = 5.0
DEFAULT_CHASE_PCT = 5.0


def _thscode(exchange: str, code: str) -> str:
    return f"{code}{_EX_MAP.get(str(exchange).lower(), '.SZ')}"


def _client():
    from eltdx import Client

    return Client.from_hosts(pool_size=8)


def _mac():
    from eltdx.mac import MacClient

    c = MacClient()
    c.connect()
    return c


def _resolve_trade_date(client) -> str:
    """竞价定版前 → 上一交易日; 之后 → 当日。周末/节假日用 workdays 归一。"""
    try:
        now = datetime.now(_CN_TZ)
        today = client.workdays.today()
        if (now.hour, now.minute) < _AUCTION_READY:
            prev = client.workdays.previous_workday(today)
            if prev is not None:
                return prev.isoformat()
        return today.isoformat()
    except Exception as e:  # noqa: BLE001
        logger.warning("交易日解析失败, 退回本地日期: %s", e)
        return datetime.now(_CN_TZ).date().isoformat()


def _universe(client) -> tuple[list[str], dict[str, str], dict[str, dict]]:
    """A 股全量代码 + 名称表 + 维表。排除 B 股 (sh900/sz200) 与非 a_share 类别。"""
    codes: list[str] = []
    names: dict[str, str] = {}
    meta: dict[str, dict] = {}
    for item in client.helpers.latest_stock_list():
        category = getattr(item, "category", "") or ""
        if category and category != "a_share":
            continue
        ex = str(getattr(item, "exchange", "") or "").lower()
        code = str(getattr(item, "code", "") or "")
        if not ex or not code:
            continue
        if ex == "sh" and code.startswith("900"):
            continue
        if ex == "sz" and code.startswith("200"):
            continue
        full = f"{ex}{code}"
        codes.append(full)
        names[full] = str(getattr(item, "name", "") or "")
        meta[full] = {
            "board": getattr(item, "board", "") or "",
            "decimal": getattr(item, "decimal", 2),
            "multiple": getattr(item, "multiple", 100),
        }
    return codes, names, meta


def _tags_for(mac, exchange: str, code: str, *, concept_limit: int = 4) -> list[str]:
    """个股标签: 行业 (board_type 12) 名称 + 概念 (board_type 4) 名称。"""
    market = 1 if exchange.lower() == "sh" else (2 if exchange.lower() == "bj" else 0)
    try:
        boards = mac.get_belong_board(market, code)
    except Exception:  # noqa: BLE001 — 标签是增强项, 失败不影响主流程
        return []
    concepts = [b.board_name for b in boards if b.board_type == 4]
    industries = [b.board_name for b in boards if b.board_type == 12]
    out: list[str] = []
    # 行业取最细的一级 (belong_board 里 12 类型通常是 [L2, L3])
    for name in reversed(industries):
        if name not in out:
            out.append(name)
            break
    for name in concepts[:concept_limit]:
        if name not in out:
            out.append(name)
    return [str(x) for x in out if x]


def _is_st(name: str) -> bool:
    return "ST" in str(name).upper() or "退" in str(name)


def get_auction_scan(
    client=None,
    *,
    mac=None,
    min_pct: float = DEFAULT_MIN_PCT,
    max_pct: float = DEFAULT_MAX_PCT,
    chase_pct: float = DEFAULT_CHASE_PCT,
    limit: int = 300,
    with_tags: bool = True,
    exclude_st: bool = False,
) -> dict:
    """全市场竞价扫描。返回 {state, trade_date, scanned, live, candidates[], chase_risk[]}。

    行字段: thscode/ticker/name/auction_pct/open_price/pre_close_price/
            open_amount(元)/last_price/amount/tags[]/is_st
    """
    own_client = client is None
    if own_client:
        client = _client()
    own_mac = False
    if with_tags and mac is None:
        try:
            mac = _mac()
            own_mac = True
        except Exception as e:  # noqa: BLE001
            logger.warning("mac 连接失败, 跳过标签: %s", e)
            mac = None

    try:
        codes, names, meta = _universe(client)
        if not codes:
            return {"state": "no_data", "message": "latest_stock_list 为空"}
        quotes = client.helpers.full_quotes(codes)
        trade_date = _resolve_trade_date(client)

        rows: list[dict] = []
        live = 0
        for q in quotes:
            last = float(getattr(q, "last_price", 0.0) or 0.0)
            op = float(getattr(q, "open_price", 0.0) or 0.0)
            prev = float(getattr(q, "pre_close_price", 0.0) or 0.0)
            if last > 0 and op > 0 and prev > 0:
                live += 1
            else:
                continue
            pct = (op / prev - 1.0) * 100.0
            if pct < min_pct:
                continue
            ex = str(getattr(q, "exchange", "") or "").lower()
            code = str(getattr(q, "code", "") or "")
            name = names.get(f"{ex}{code}") or ""
            if exclude_st and _is_st(name):
                continue
            rows.append(
                {
                    "thscode": _thscode(ex, code),
                    "ticker": code,
                    "name": name,
                    "auction_pct": round(pct, 4),
                    "open_price": op,
                    "pre_close_price": prev,
                    "open_amount": float(getattr(q, "open_amount_yuan", 0.0) or 0.0),
                    "last_price": last,
                    "amount": float(getattr(q, "amount", 0.0) or 0.0),
                    "is_st": _is_st(name),
                    "tags": [],
                    "_ex": ex,
                    "_code": code,
                }
            )

        candidates = [r for r in rows if min_pct <= r["auction_pct"] < max_pct]
        chase_risk = [r for r in rows if r["auction_pct"] >= chase_pct]
        candidates.sort(key=lambda r: (-r["open_amount"], -r["auction_pct"]))
        chase_risk.sort(key=lambda r: -r["auction_pct"])
        candidates = candidates[:limit]
        chase_risk = chase_risk[:limit]

        if mac is not None:
            for r in candidates + chase_risk:
                if not r["tags"]:
                    r["tags"] = _tags_for(mac, r["_ex"], r["_code"])

        for r in candidates + chase_risk:
            r.pop("_ex", None)
            r.pop("_code", None)

        return {
            "state": "ok",
            "trade_date": trade_date,
            "scanned": len(codes),
            "live": live,
            "min_pct": min_pct,
            "max_pct": max_pct,
            "chase_pct": chase_pct,
            "count": len(candidates),
            "candidates": candidates,
            "chase_risk_count": len(chase_risk),
            "chase_risk": chase_risk,
            "source": "eltdx:helpers.full_quotes",
        }
    finally:
        if own_mac and mac is not None:
            try:
                mac.close()
            except Exception:  # noqa: BLE001
                pass
        if own_client:
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass


def get_auction_benchmark(client=None, **kwargs) -> dict:
    """盘前风向标替代品 — 产出 fuyao short_term_benchmark 形状的容器。

    返回 {"date": "YYYY-MM-DD", "item": [{thscode,ticker,name,auction_pct,tags}]}
    面板 services/auction_benchmark._base_payload 直接吃这个结构, 之后自己做
    day0_oc/day0_pct/d1_pct 的本地 enrich 与按日落缓存。
    """
    scan = get_auction_scan(client=client, **kwargs)
    if scan.get("state") != "ok":
        return scan
    items = [
        {
            "thscode": r["thscode"],
            "ticker": r["ticker"],
            "name": r["name"],
            "auction_pct": r["auction_pct"],
            "tags": r["tags"],
        }
        for r in scan["candidates"]
    ]
    return {
        "state": "ok",
        "date": scan["trade_date"],
        "count": len(items),
        "item": items,
        "chase_risk": [
            {"thscode": r["thscode"], "name": r["name"], "auction_pct": r["auction_pct"]}
            for r in scan["chase_risk"][:20]
        ],
        "scanned": scan["scanned"],
        "source": "eltdx:helpers.full_quotes",
    }


# ---------------------------------------------------------------------------
# 插件 provider 门面 (lead 指定的 A 部分契约)
# ---------------------------------------------------------------------------
# 规则字段 (全部可覆盖; 缺省即面板默认口径)
DEFAULT_RULES: dict[str, Any] = {
    "min_pct": DEFAULT_MIN_PCT,      # 竞价高开下限 (%)
    "max_pct": DEFAULT_MAX_PCT,      # 竞价高开上限 (%)
    "chase_pct": DEFAULT_CHASE_PCT,  # 追高风险阈值 (%)
    "limit": 300,                    # 每桶最多返回条数
    "with_tags": True,               # 是否逐股取 行业+概念 标签 (关掉可省 ~12s)
    "exclude_st": False,             # 是否剔除 ST/退市
    "min_open_amount": 0.0,          # 竞价成交额下限 (元), 用来滤掉僵尸股
}


def get_auction_candidates(
    client=None, date: str | None = None, rules: dict | None = None, *, mac=None
) -> dict:
    """provider 门面: get_auction_candidates(date, rules)。

    返回面板 backend/app/services/auction_benchmark.py 可直接消费的结构:
      {state, trade_date, requested_date, count, item:[{thscode,ticker,name,auction_pct,tags}],
       chase_risk:[...], scanned, live, source}

    硬限制: 竞价快照来自 helpers.full_quotes, **只能拿「当前」快照**。
    date 若早于当日快照所属交易日 → 返回 state=no_data (协议不支持历史竞价快照);
    历史日请走面板缓存补写 (build_panel_cache_payload + 写 data/auction_benchmark/date=*.json)。
    """
    merged = dict(DEFAULT_RULES)
    if rules:
        unknown = sorted(set(rules) - set(DEFAULT_RULES))
        if unknown:
            raise ValueError(f"rules 含未知键: {unknown} (可选 {sorted(DEFAULT_RULES)})")
        merged.update({k: v for k, v in rules.items() if v is not None})

    scan = get_auction_scan(
        client=client,
        mac=mac,
        min_pct=float(merged["min_pct"]),
        max_pct=float(merged["max_pct"]),
        chase_pct=float(merged["chase_pct"]),
        limit=int(merged["limit"]),
        with_tags=bool(merged["with_tags"]),
        exclude_st=bool(merged["exclude_st"]),
    )
    if scan.get("state") != "ok":
        return scan

    floor = float(merged["min_open_amount"] or 0.0)
    cands = [r for r in scan["candidates"] if r["open_amount"] >= floor]
    chase = [r for r in scan["chase_risk"] if r["open_amount"] >= floor]

    want = str(date)[:10] if date else scan["trade_date"]
    if want != scan["trade_date"]:
        return {
            "state": "no_data",
            "message": (
                f"目标日 {want} 无法回放: 竞价快照仅来自 full_quotes 实时快照 "
                f"(当前 {scan['trade_date']}); 历史日请改用面板缓存补写"
            ),
            "requested_date": want,
            "trade_date": scan["trade_date"],
            "source": scan.get("source"),
        }

    items = [
        {
            "thscode": r["thscode"],
            "ticker": r["ticker"],
            "name": r["name"],
            "auction_pct": r["auction_pct"],
            "tags": r["tags"],
        }
        for r in cands
    ]
    return {
        "state": "ok",
        "requested_date": want,
        "trade_date": scan["trade_date"],
        "count": len(items),
        "item": items,
        "chase_risk": [
            {"thscode": r["thscode"], "name": r["name"], "auction_pct": r["auction_pct"]}
            for r in chase[:20]
        ],
        "chase_risk_count": len(chase),
        "scanned": scan["scanned"],
        "live": scan["live"],
        "rules": merged,
        "source": scan.get("source"),
    }


def build_panel_cache_payload(client=None, date: str | None = None, **kwargs) -> dict:
    """产出可直接 json.dump 到 data/auction_benchmark/date=YYYY-MM-DD.json 的缓存体。

    ⚠ 面板 _raw_items 读的是 key **"item"**, _base_payload 读 data["date"],
    所以缓存体键必须是 {state, requested_date, trade_date, count, item:[...]},
    item 行只需 thscode/ticker/name/auction_pct/tags (day0_oc 等由面板自己算)。
    历史日缓存命中时面板**完全不加载 provider**, 这是唯一不改代码即可显示的路子。
    """
    out = get_auction_candidates(client=client, date=date, **kwargs)
    if out.get("state") != "ok":
        return out
    return {
        "state": "ok",
        "requested_date": out["requested_date"],
        "trade_date": out["trade_date"],
        "count": out["count"],
        "item": out["item"],
    }



def main() -> int:
    import sys
    import time

    t0 = time.time()
    selftest = "--selftest" in sys.argv
    scan = get_auction_scan()
    elapsed = time.time() - t0
    if scan.get("state") != "ok":
        print(f"[auction] FAIL state={scan.get('state')} {scan.get('message')}")
        return 1
    print(
        f"[auction] trade_date={scan['trade_date']} scanned={scan['scanned']} "
        f"live={scan['live']} elapsed={elapsed:.2f}s"
    )
    print(
        f"[auction] candidates(1%~5%)={scan['count']} "
        f"chase_risk(>=5%)={scan['chase_risk_count']}"
    )
    for r in scan["candidates"][:8]:
        print(
            f"    {r['thscode']} {r['name']} open {r['auction_pct']:+.2f}% "
            f"open_amt={r['open_amount']/1e4:.0f}万 tags={r['tags'][:3]}"
        )
    for r in scan["chase_risk"][:3]:
        print(f"    [chase] {r['thscode']} {r['name']} open {r['auction_pct']:+.2f}%")
    bench = get_auction_benchmark()
    print(f"[auction] benchmark payload: date={bench['date']} item={bench['count']}")

    if selftest:
        t = time.time()
        short = get_auction_candidates(rules={"with_tags": False})
        print(f"[auction] facade(no-tags) state={short.get('state')} count={short.get('count')} "
              f"trade_date={short.get('trade_date')} elapsed={time.time()-t:.2f}s")
        if short.get("state") != "ok":
            return 1
        first = short["item"][0] if short["item"] else {}
        print(f"    first={first.get('thscode')} {first.get('name')} "
              f"open {first.get('auction_pct'):+.2f}% tags={first.get('tags')}")
        cache = build_panel_cache_payload(rules={"with_tags": False})
        print(f"[auction] panel cache payload keys={sorted(cache)} count={cache.get('count')}")
        if cache.get("item") and len(cache["item"]) != len(short["item"]):
            print("[auction] WARN 缓存体条目数与门面不一致")
            return 1
        beyond = get_auction_candidates(date="2026-01-05", rules={"with_tags": False})
        print(f"[auction] out-of-window probe state={beyond.get('state')} msg={beyond.get('message')}")
        try:
            get_auction_candidates(rules={"nope": 1})
        except ValueError as e:
            print(f"[auction] rules 校验 OK: {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
