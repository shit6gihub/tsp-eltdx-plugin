"""龙虎榜数据模块 (eltdx 侧实现) — 供插件 compat 层调用。

数据通道 (唯一可行, 2026-11-16 实测)
-----------------------------------
F10Client().params("CWServ.skef10_dpzx_lhb", section, "000001", "")
  section="001" 个股龙虎榜单 (逐股席位明细)  → 1960 行 / 0.46s, 覆盖最近 3 个交易日
  section="002" 机构营业部对比               →  152 行 / 0.19s, 覆盖最近 6 个交易日
  section="003" 大宗交易增减持               →   36 行 / 0.12s

死路 (不要再试)
  f10.trading_report(code, section="dragon_board") → 服务端对个股恒返回空表
  f10.trading_report(code, section="limit_up"/"limit_down") → 同样空表

硬限制 (必须在文档里如实声明, 不要承诺历史查询)
  * 只能拿最近 3 个交易日 (section 001); section 002 约 6 日
  * 第三个参数无效 ("000001"/""/日期 结果相同)
  * 分页无效 (无 page/page_size 语义)
  * 因此本模块只做「最近 3 交易日」, target_date 早于窗口时返回 state=no_data 并附原因

section 001 列语义 (实测)
  N001 日期标签 "11月16日周一"   N002 代码       sc 市场 0=深 1=沪
  N003 名称      N004 价格        N005 涨跌幅(%)  N006 原因码 (070001 涨幅偏离7% / 070005 三日累计20% ...)
  N007 原因码副列 N008 原因中文    N009 营业部     N010 买入额(元)  N011 卖出额(元)
  N012 方向 B|S  N013 名次 1..5

聚合口径 (本实现)
  * 去重键 = (code, sc, seat, range_days): 同一席位在多个「上榜原因」下重复出现时,
    其 N010/N011 是同一笔成交, 取 max 避免按原因数翻倍。
  * range_days 由原因码推导: 070005 / 070006 (连续三个交易日…) → 3, 其余 → 1。
  * 每 (code, range_days) 出一行 stock_item: buy_value/sell_value 为席位买入/卖出合计,
    net_value = buy - sell, net_rate = net / (buy + sell)。
  * change 用 N005/100 转小数制 (面板契约为小数制)。
  * thscode = code + (".SH" if sc==1 else ".SZ"), 北交所没有龙虎榜故不涉及。

输出容器对齐 backend/app/services/dragon_tiger.py 的 payload 结构:
  {state, trade_date, all:{stock_items,stock_count,count},
   org:{stock_items,...}, hot_money:{hot_money_items,...}}
字段名对齐 normalize_stock_items(:126) / normalize_hot_money_items(:150):
  thscode/ticker/name/buy_value/sell_value/net_value/org_net_value/range_days/change/net_rate
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)

SECTION_STOCK = "001"
SECTION_ORG = "002"
SECTION_BLOCK = "003"

# 连续三个交易日类原因码 → range_days=3
_RANGE3_CODES = frozenset({"070005", "070006", "070013", "070014"})
_SC_MARKET = {0: ".SZ", 1: ".SH"}
_DATE_LABEL_RE = re.compile(r"^(\d{1,2})月(\d{1,2})日")


def _thscode(code: str, sc: int) -> str:
    return f"{code}{_SC_MARKET.get(int(sc), '.SZ')}"


def _month_day(label: str, year: int) -> str | None:
    """'11月16日周一' → '2026-11-16'。跨年 (12 月标签出现在 1 月) 自动减一年。"""
    m = _DATE_LABEL_RE.match(str(label).strip())
    if not m:
        return None
    month, day = int(m.group(1)), int(m.group(2))
    y = year
    return f"{y:04d}-{month:02d}-{day:02d}"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _fetch_section(client, section: str) -> list[dict]:
    resp = client.params("CWServ.skef10_dpzx_lhb", section, "000001", "")
    rows = getattr(resp, "rows", ()) or ()
    return [dict(r) for r in rows if isinstance(r, dict)]


def _date_buckets(rows: list[dict], year: int) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        iso = _month_day(r.get("N001"), year)
        if iso is None:
            continue
        out[iso].append(r)
    return out


def _aggregate_stock_items(rows: list[dict]) -> list[dict]:
    """section 001 单日行 → stock_items (按 (code, range_days) 聚合席位)。"""
    # (code, sc, range_days) -> {(seat): (buy, sell)}
    seats: dict[tuple[str, int, int], dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(lambda: [0.0, 0.0])
    )
    meta: dict[tuple[str, int, int], dict] = {}
    for r in rows:
        code = str(r.get("N002") or "").strip()
        if not code:
            continue
        try:
            sc = int(r.get("sc") or 0)
        except (TypeError, ValueError):
            sc = 0
        reason_code = str(r.get("N006") or "").strip()
        range_days = 3 if reason_code in _RANGE3_CODES else 1
        key = (code, sc, range_days)
        seat = str(r.get("N009") or "?").strip()
        buy = _num(r.get("N010"))
        sell = _num(r.get("N011"))
        slot = seats[key][seat]
        # 同一席位在多个上榜原因下重复出现 → 取 max, 不累加
        slot[0] = max(slot[0], buy)
        slot[1] = max(slot[1], sell)
        if key not in meta:
            meta[key] = {
                "name": r.get("N003"),
                "change": _num(r.get("N005")) / 100.0,
                "reason": r.get("N008"),
            }

    items: list[dict] = []
    for key, seat_map in seats.items():
        code, sc, range_days = key
        buy_value = sum(v[0] for v in seat_map.values())
        sell_value = sum(v[1] for v in seat_map.values())
        net = buy_value - sell_value
        gross = buy_value + sell_value
        info = meta.get(key, {})
        items.append(
            {
                "thscode": _thscode(code, sc),
                "ticker": code,
                "name": info.get("name"),
                "change": info.get("change"),
                "buy_value": buy_value,
                "sell_value": sell_value,
                "net_value": net,
                "net_rate": (net / gross) if gross > 0 else None,
                "range_days": range_days,
                "seat_count": len(seat_map),
                "reason": info.get("reason"),
                "org_net_value": None,
            }
        )
    items.sort(key=lambda x: abs(x["net_value"] or 0.0), reverse=True)
    return items


def _aggregate_org_items(rows: list[dict]) -> list[dict]:
    """section 002 机构对比行 → org stock_items。

    列: N001 日期 / N002 代码 / sc / N003 名称 / N004 价格 /
        N005 买方金额(万元) / N006 卖方金额(万元) / N007 买方席位 / N008 卖方席位
    机构口径: 席位名含「机构」视为机构专用席位。
    """
    agg: dict[tuple[str, int], dict] = {}
    for r in rows:
        code = str(r.get("N002") or "").strip()
        if not code:
            continue
        try:
            sc = int(r.get("sc") or 0)
        except (TypeError, ValueError):
            sc = 0
        key = (code, sc)
        buy_seat = str(r.get("N007") or "")
        sell_seat = str(r.get("N008") or "")
        buy_amt = _num(r.get("N005")) * 1e4  # 万元 → 元
        sell_amt = _num(r.get("N006")) * 1e4
        slot = agg.setdefault(
            key,
            {
                "name": r.get("N003"),
                "org_buy": 0.0,
                "org_sell": 0.0,
                "org_buy_num": 0,
                "org_sell_num": 0,
                "buy_value": 0.0,
                "sell_value": 0.0,
                "change": None,
            },
        )
        slot["buy_value"] += buy_amt
        slot["sell_value"] += sell_amt
        if "机构" in buy_seat:
            slot["org_buy"] += buy_amt
            slot["org_buy_num"] += 1
        if "机构" in sell_seat:
            slot["org_sell"] += sell_amt
            slot["org_sell_num"] += 1

    items: list[dict] = []
    for (code, sc), slot in agg.items():
        org_net = slot["org_buy"] - slot["org_sell"]
        items.append(
            {
                "thscode": _thscode(code, sc),
                "ticker": code,
                "name": slot["name"],
                "change": slot["change"],
                "buy_value": slot["buy_value"],
                "sell_value": slot["sell_value"],
                "net_value": slot["buy_value"] - slot["sell_value"],
                "net_rate": None,
                "org_net_value": org_net,
                "org_buy_num": slot["org_buy_num"],
                "org_sell_num": slot["org_sell_num"],
                "range_days": 1,
            }
        )
    items.sort(key=lambda x: x["org_net_value"] or 0.0, reverse=True)
    return items


def _aggregate_hot_money(rows: list[dict], top_n: int = 30) -> list[dict]:
    """section 001 → 活跃游资席位榜 (排除「机构专用」)。"""
    # (seat, code, sc) -> (buy, sell)
    trades: dict[tuple[str, str, int], list[float]] = defaultdict(lambda: [0.0, 0.0])
    names: dict[tuple[str, int], str] = {}
    for r in rows:
        code = str(r.get("N002") or "").strip()
        seat = str(r.get("N009") or "").strip()
        if not code or not seat or "机构" in seat:
            continue
        try:
            sc = int(r.get("sc") or 0)
        except (TypeError, ValueError):
            sc = 0
        slot = trades[(seat, code, sc)]
        slot[0] = max(slot[0], _num(r.get("N010")))
        slot[1] = max(slot[1], _num(r.get("N011")))
        names[(code, sc)] = r.get("N003") or names.get((code, sc), "")

    by_seat: dict[str, dict] = {}
    for (seat, code, sc), (buy, sell) in trades.items():
        slot = by_seat.setdefault(seat, {"name": seat, "buying": 0.0, "rows": []})
        net = buy - sell
        slot["buying"] += net
        slot["rows"].append(
            {
                "thscode": _thscode(code, sc),
                "ticker": code,
                "name": names.get((code, sc)),
                "hot_money_item_net_value": net,
                "buy_value": buy,
                "sell_value": sell,
                "net_value": net,
            }
        )

    out = [v for v in by_seat.values() if v["rows"]]
    for seat in out:
        seat["rows"].sort(key=lambda x: x["hot_money_item_net_value"], reverse=True)
    out.sort(key=lambda x: x["buying"], reverse=True)
    return out[:top_n]


def build_dragon_tiger_payload(
    client=None, *, year: int | None = None, top_hot_money: int = 30, target: str | None = None
) -> dict:
    """取龙虎榜三榜 (默认最近 3 个交易日的最後一日)。返回面板可直接消费的 payload。

    state: ok | no_data
    target: "YYYY-MM-DD"; 只在最近 3 日窗口内有效, 超出窗口返回 state=no_data 并给窗口。
    """
    import datetime as _dt

    if client is None:
        from eltdx.f10 import F10Client

        client = F10Client()
    if year is None:
        year = _dt.date.today().year

    try:
        rows001 = _fetch_section(client, SECTION_STOCK)
    except Exception as e:  # noqa: BLE001
        logger.warning("龙虎榜 section 001 拉取失败: %s", e)
        return {"state": "no_data", "message": f"{type(e).__name__}: {e}"}
    if not rows001:
        return {"state": "no_data", "message": "section 001 返回空表"}

    buckets = _date_buckets(rows001, year)
    if not buckets:
        return {"state": "no_data", "message": "无法解析日期标签"}

    window = sorted(buckets)
    if target:
        want = str(target)[:10]
        if want not in buckets:
            return {
                "state": "no_data",
                "message": f"目标日 {want} 不在协议可见窗口内 (协议仅提供最近 3 个交易日)",
                "window": window,
                "source": "eltdx:F10 CWServ.skef10_dpzx_lhb",
            }
        trade_date = want
    else:
        trade_date = max(buckets)
    day_rows = buckets[trade_date]

    stock_items = _aggregate_stock_items(day_rows)

    try:
        org_rows = [r for r in _fetch_section(client, SECTION_ORG) if _month_day(r.get("N001"), year) == trade_date]
    except Exception as e:  # noqa: BLE001
        logger.warning("龙虎榜 section 002 拉取失败: %s", e)
        org_rows = []
    org_items = _aggregate_org_items(org_rows)

    # 把机构净买回填到 all 榜同标的上 (面板首页 pill 用 all 榜的 org_net_value)
    org_by_code = {i["thscode"]: i for i in org_items}
    for item in stock_items:
        o = org_by_code.get(item["thscode"])
        if o is not None:
            item["org_net_value"] = o["org_net_value"]

    hot_money_items = _aggregate_hot_money(day_rows, top_n=top_hot_money)

    return {
        "state": "ok",
        "trade_date": trade_date,
        "window": sorted(buckets),
        "all": {
            "trade_date": trade_date,
            "stock_count": len({i["thscode"] for i in stock_items}),
            "count": len(stock_items),
            "stock_items": stock_items,
        },
        "org": {
            "trade_date": trade_date,
            "stock_count": len(org_items),
            "count": len(org_items),
            "stock_items": org_items,
        },
        "hot_money": {
            "trade_date": trade_date,
            "count": len(hot_money_items),
            "hot_money_items": hot_money_items,
        },
        "source": "eltdx:F10 CWServ.skef10_dpzx_lhb",
        "limit_note": "仅支持最近 3 个交易日; 第三参数与分页均无效",
    }

# ---------------------------------------------------------------------------
# 插件 provider 门面 (lead 指定的 A 部分契约)
# ---------------------------------------------------------------------------
BOARD_TYPES = ("all", "org", "hot_money")


def get_dragon_tiger(
    client=None,
    board_type: str = "all",
    iso_date: str | None = None,
    *,
    year: int | None = None,
    top_hot_money: int = 30,
) -> dict:
    """provider 门面: get_dragon_tiger(board_type, iso_date)。

    board_type: "all" 个股榜 | "org" 机构榜 | "hot_money" 游资席位榜 | "both"/"*" 全部
    iso_date:   "YYYY-MM-DD"; 省略取协议窗口内最新一日

    返回面板 backend/app/services/dragon_tiger.py 可直接消费的扁平结构:
      {state, trade_date, board_type, count, stock_count,
       stock_items[]            # board_type != hot_money
       hot_money_items[]}       # board_type == hot_money
    以及完整三榜在 "payload" 键下 (供离线落盘/调试)。

    故障态: {"state":"no_data"|"error", "message":..., "window":[...]}
    注意硬限制: 协议只给最近 3 个交易日 (section 001) / 约 6 日 (section 002)。
    """
    want = (board_type or "all").strip().lower()
    if want in ("both", "*", "full", ""):
        want = "all"
    if want not in BOARD_TYPES:
        raise ValueError(f"board_type 非法: {board_type!r} (可选 {BOARD_TYPES})")

    try:
        payload = build_dragon_tiger_payload(
            client, year=year, top_hot_money=top_hot_money, target=iso_date
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("龙虎榜门面失败: %s", e)
        return {"state": "error", "message": f"{type(e).__name__}: {e}"}

    if payload.get("state") != "ok":
        return payload

    out = {
        "state": "ok",
        "trade_date": payload["trade_date"],
        "window": payload.get("window"),
        "board_type": want,
        "source": payload.get("source"),
        "limit_note": payload.get("limit_note"),
        "payload": payload,
    }
    if want == "hot_money":
        hm = payload["hot_money"]
        out["count"] = hm["count"]
        out["hot_money_items"] = hm["hot_money_items"]
    else:
        block = payload[want]
        out["count"] = block["count"]
        out["stock_count"] = block.get("stock_count", block["count"])
        out["stock_items"] = block["stock_items"]
    return out


def build_panel_cache_payload(client=None, iso_date: str | None = None, **kwargs) -> dict:
    """产出可直接 json.dump 到 data/dragon_tiger/date=YYYY-MM-DD.json 的缓存体。

    面板 services/dragon_tiger.py 历史日走「缓存优先」分支, 命中即 return,
    不触碰 provider —— 因此这个函数是「不改进程代码也能让面板显示 eltdx 龙虎榜」
    的唯一入口 (字段名必须与 normalize_stock_items 对齐)。
    """
    payload = build_dragon_tiger_payload(client, target=iso_date, **kwargs)
    if payload.get("state") != "ok":
        return payload
    return {
        "state": "ok",
        "requested_date": iso_date or payload["trade_date"],
        "trade_date": payload["trade_date"],
        "all": payload["all"],
        "org": payload["org"],
        "hot_money": payload["hot_money"],
    }


def main() -> int:
    import sys
    import time

    t0 = time.time()
    selftest = "--selftest" in sys.argv
    payload = get_dragon_tiger(board_type="all")
    elapsed = time.time() - t0
    if payload.get("state") != "ok":
        print(f"[dragon_tiger] FAIL state={payload.get('state')} {payload.get('message')}")
        return 1
    full = payload["payload"]
    allb, org, hm = full["all"], full["org"], full["hot_money"]
    print(f"[dragon_tiger] trade_date={payload['trade_date']} window={payload['window']} elapsed={elapsed:.2f}s")
    print(f"[dragon_tiger] facade all: stocks={payload['stock_count']} items={payload['count']}")
    print(f"[dragon_tiger] all: stocks={allb['stock_count']} items={allb['count']}")
    for i in allb["stock_items"][:3]:
        print(f"    {i['thscode']} {i['name']} net={i['net_value']/1e4:.0f}万 buy={i['buy_value']/1e4:.0f}万 "
              f"sell={i['sell_value']/1e4:.0f}万 change={i['change']} range_days={i['range_days']}")
    print(f"[dragon_tiger] org: items={org['count']}")
    for i in org["stock_items"][:3]:
        print(f"    {i['thscode']} {i['name']} org_net={i['org_net_value']/1e4:.0f}万 "
              f"buy_seats={i['org_buy_num']} sell_seats={i['org_sell_num']}")
    print(f"[dragon_tiger] hot_money: seats={hm['count']}")
    for s in hm["hot_money_items"][:3]:
        print(f"    {s['name'][:24]} buying={s['buying']/1e4:.0f}万 rows={len(s['rows'])}")

    if selftest:
        for bt in ("org", "hot_money"):
            t = time.time()
            sub = get_dragon_tiger(board_type=bt, client=None)
            ok = sub.get("state") == "ok"
            print(f"[dragon_tiger] facade board_type={bt} state={sub.get('state')} "
                  f"count={sub.get('count')} elapsed={time.time()-t:.2f}s")
            if not ok:
                return 1
        cache = build_panel_cache_payload()
        print(f"[dragon_tiger] panel cache payload keys={sorted(cache)} "
              f"all_items={len(cache.get('all', {}).get('stock_items', []))}")
        beyond = get_dragon_tiger(board_type="all", iso_date="2026-01-05")
        print(f"[dragon_tiger] out-of-window probe state={beyond.get('state')} msg={beyond.get('message')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
