"""板块 (行业 / 概念) 数据模块 (eltdx 侧实现) — 供插件 compat 层调用。

面板现状 (为什么要这个模块)
--------------------------
面板的「行业 / 概念」维度**不来自 provider 契约**，而是读扩展数据表
ext_data/ext_hy_ths（所属同花顺行业）与 ext_data/ext_gn_ths（所属概念），
这两张表由 services/ext_presets.py:69/:124 的出厂配方从 fuyao HTTP 拉取
（_FUYAO_BASE=https://fuyao.aicubes.cn、/api/a-share-index/catalog/ths-index-list
 + /constituents/ths-stock-list、fanout 逐板块 8 并发）。消费方：
  - 行业板块分析页 custom/industry-analysis/Page.tsx:34 KEYWORDS、
    :39 IndustryLevel=1|2|3、:253 industryLevelName(按 "-" 拆级)、
    :39 PRESET_INDUSTRY_ID='ext_hy_ths'
  - 概念板块分析页 custom/concept-analysis/Page.tsx
  - overview 概念/行业排名 market_overview_builder.py:277 _dimension_rank
    （:556 concept / :557 industry, level=2）
  - RPS 轮动 rps_rotation.py:97 _load_concept_map_df
  - 盘中板块轮动 sector_rotation.py（走 _load_concept_map_df，零新增源）

本模块用 eltdx 的 MAC 板块协议产出一份**等价快照**，让 ext_hy_ths / ext_gn_ths
可以不再依赖 fuyao：

  BoardType.YJ_LEVEL1 = 7  申万一级 30 个   (881385 银行 / 881001 煤炭 …)
  BoardType.YJ_LEVEL2 = 8  申万二级 128 个  (与 BoardType.HY 完全同集)
  BoardType.YJ_LEVEL3 = 9  申万三级 345 个  (与 BoardType.HY2 完全同集)
  BoardType.GN        = 3  概念 269 个     (880671 中特估 / 880564 白酒概念 …)
  BoardType.FG        = 4  风格 158 个
  BoardType.DQ        = 5  地区  32 个

关键实测结论（2026-11-17）
  * get_board_members(板块码) 每板块 0.04~0.15s，是**成分股唯一可行通道**；
    helpers.board_members 与 f10.index_members 对 880/881 行业指数均为 0 行。
  * 三级层级用「成分股集合包含度」递推确定父级：子板块成分 100% 落在父板块内，
    实测 345 个三级板块全部找到 >=0.85 覆盖的二级父级，128 个二级全部找到一级父级，
    每只股票恰好 1 条 3 级路径（无歧义）。
  * 概念无层级，反转为「股票 → 所属概念;…」（分隔符 ";"，与 fuyao 出厂配方一致）；
    5460 只股票 / 269 个概念 / 每股中位 7 个概念 / 最多 49 个，全量拉取 26.6s。
  * get_belong_board(market, code) 也可拿个股板块（实测 600519 → 20 个板块，
    board_type 3=地区 / 4=概念 / 5=风格 / 12=行业），但逐股调用全市场要 3~5 分钟，
    只适合单股详情页，不适合做全量快照 → 本模块默认走「板块→成分股」反转。

输出
----
1. build_industry_rows()   → 可直接 POST /api/ext-data/ext_hy_ths/ingest 的行
   字段: symbol / code / 股票代码 / 股票简称 / 所属行业   （"一级-二级-三级"）
2. build_concept_rows()    → 可直接 POST /api/ext-data/ext_gn_ths/ingest 的行
   字段: symbol / code / 股票代码 / 股票简称 / 所属概念   （分号拼接）
3. build_board_snapshot()  → 板块级行情快照（面板暂无消费方，可选增强）
4. get_board_ranking()     → 申万一级/二级/三级/概念 的板块排名（覆盖 overview
   industry_rank / concept_rank 的上游，亦可喂 market_recap 的板块段）

注意: 写入需要表已存在且字段名一致；面板侧 ext_hy_ths 的字段是
「所属同花顺行业」而不是「所属行业」，所以 ingest 前请用 map_industry_fields()
把列名改写（本模块提供该函数，不改变 ext_* 配置本身）。
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterable

logger = logging.getLogger(__name__)

_MARKET_SUFFIX = {0: ".SZ", 1: ".SH", 2: ".BJ"}

# 申万三级层级 (BoardType) — 名称与实际含义已实测校准
LEVEL_BOARDS = {1: 7, 2: 8, 3: 9}  # YJ_LEVEL1 / YJ_LEVEL2 / YJ_LEVEL3
BOARD_GN = 3
BOARD_FG = 4
BOARD_DQ = 5


def _thscode(market: int, code: str) -> str:
    return f"{code}{_MARKET_SUFFIX.get(int(market), '.SZ')}"


def _mac():
    from eltdx.mac import MacClient

    c = MacClient()
    c.connect()
    return c


def _members(mac, board_symbol: str) -> list[Any]:
    try:
        return list(mac.get_board_members(board_symbol))
    except Exception as e:  # noqa: BLE001
        logger.warning("get_board_members(%s) 失败: %s", board_symbol, e)
        return []


def _board_index(mac, board_type: int) -> dict[str, tuple[str, set[tuple[int, str]]]]:
    """板块码 → (板块名, {(market, code)} 成分股集合)。"""
    out: dict[str, tuple[str, set[tuple[int, str]]]] = {}
    for b in mac.get_board_list(board_type):
        members = _members(mac, b.code)
        out[b.code] = (b.name, {(int(m.market), str(m.code)) for m in members})
    return out


def _best_parent(child: set, parents: dict[str, tuple[str, set]]) -> tuple[str, str, float] | None:
    """按成分股包含度找父板块: 返回 (板码, 板名, 覆盖率)。"""
    best: tuple[str, str, float] | None = None
    if not child:
        return None
    for code, (name, members) in parents.items():
        if not members:
            continue
        cov = len(members & child) / len(child)
        if best is None or cov > best[2]:
            best = (code, name, cov)
    return best


def build_industry_tree(mac=None, *, min_coverage: float = 0.85) -> dict:
    """产出 {stock_key: (一级, 二级, 三级)} + 名称表 + 统计。

    层级用成分股包含度递推；覆盖度 < min_coverage 的父子关系视为不可信并跳过
    （实测三级/二级命中率 100%，跳过数为 0）。
    """
    own = False
    if mac is None:
        mac = _mac()
        own = True
    t0 = time.time()
    try:
        l1 = _board_index(mac, LEVEL_BOARDS[1])
        l2 = _board_index(mac, LEVEL_BOARDS[2])
        l3 = _board_index(mac, LEVEL_BOARDS[3])
        t_lists = time.time() - t0

        tree: dict[tuple[int, str], list[str]] = {}
        names: dict[tuple[int, str], str] = {}
        skipped = 0
        for c3, (n3, s3) in l3.items():
            if not s3:
                skipped += 1
                continue
            p2 = _best_parent(s3, l2)
            if p2 is None or p2[2] < min_coverage:
                skipped += 1
                continue
            s2 = l2[p2[0]][1]
            p1 = _best_parent(s2, l1)
            if p1 is None or p1[2] < min_coverage:
                skipped += 1
                continue
            for key in s3:
                names[key] = names.get(key, "")
                tree[key] = [p1[1], p2[1], n3]

        # 名称: 从成分股快照里抓（MacQuoteField.name）
        for c3 in l3:
            for m in _members(mac, c3):
                key = (int(m.market), str(m.code))
                if str(getattr(m, "name", "") or ""):
                    names[key] = str(m.name)

        return {
            "tree": tree,
            "names": names,
            "counts": {
                "l1": len(l1),
                "l2": len(l2),
                "l3": len(l3),
                "stocks": len(tree),
                "skipped": skipped,
            },
            "elapsed": round(time.time() - t0, 2),
            "t_lists": round(t_lists, 2),
        }
    finally:
        if own:
            try:
                mac.close()
            except Exception:  # noqa: BLE001
                pass


def build_concept_map(mac=None, *, board_type: int = BOARD_GN, separator: str = ";") -> dict:
    """产出 {stock_key: [概念名…]} + 名称表 + 统计（板块→成分股反转）。"""
    own = False
    if mac is None:
        mac = _mac()
        own = True
    t0 = time.time()
    try:
        boards = list(mac.get_board_list(board_type))
        inv: dict[tuple[int, str], list[str]] = {}
        names: dict[tuple[int, str], str] = {}
        empty = 0
        for b in boards:
            members = _members(mac, b.code)
            if not members:
                empty += 1
                continue
            for m in members:
                key = (int(m.market), str(m.code))
                inv.setdefault(key, []).append(b.name)
                if str(getattr(m, "name", "") or ""):
                    names[key] = str(m.name)
        return {
            "map": inv,
            "names": names,
            "separator": separator,
            "counts": {
                "boards": len(boards),
                "empty_boards": empty,
                "stocks": len(inv),
                "memberships": sum(len(v) for v in inv.values()),
            },
            "elapsed": round(time.time() - t0, 2),
        }
    finally:
        if own:
            try:
                mac.close()
            except Exception:  # noqa: BLE001
                pass


def build_industry_rows(mac=None, **kwargs) -> list[dict]:
    """ext_data 行: symbol / code / 股票代码 / 股票简称 / 所属行业("一级-二级-三级")。"""
    data = build_industry_tree(mac=mac, **kwargs)
    rows: list[dict] = []
    for (market, code), path in sorted(data["tree"].items(), key=lambda kv: kv[0][1]):
        thscode = _thscode(market, code)
        rows.append(
            {
                "symbol": thscode,
                "code": code,
                "股票代码": thscode,
                "股票简称": data["names"].get((market, code), ""),
                "所属行业": "-".join(path),
            }
        )
    data["rows"] = rows
    return rows


def build_concept_rows(mac=None, **kwargs) -> list[dict]:
    """ext_data 行: symbol / code / 股票代码 / 股票简称 / 所属概念(分号拼接)。"""
    data = build_concept_map(mac=mac, **kwargs)
    sep = data["separator"]
    rows: list[dict] = []
    for (market, code), concepts in sorted(data["map"].items(), key=lambda kv: kv[0][1]):
        thscode = _thscode(market, code)
        uniq = list(dict.fromkeys(concepts))
        rows.append(
            {
                "symbol": thscode,
                "code": code,
                "股票代码": thscode,
                "股票简称": data["names"].get((market, code), ""),
                "所属概念": sep.join(uniq),
            }
        )
    return rows


def map_industry_fields(rows: Iterable[dict], target_field: str = "所属同花顺行业") -> list[dict]:
    """把 "所属行业" 列改名为面板 ext_hy_ths 配置的列名（默认「所属同花顺行业」）。

    面板出厂 ext_hy_ths 的字段是小写 h 的「所属同花顺行业」（见
    services/ext_presets.py:130），而 preferences.py:164 的默认值是
    「所属同话顺行业」（上游 typo）。ingest 前务必用目标表 /api/ext-data 返回的
    fields 里的真实列名调用本函数，避免 400「第 N 行缺少字段」。
    """
    out: list[dict] = []
    for r in rows:
        r2 = dict(r)
        value = r2.pop("所属行业", None)
        if value is not None:
            r2[target_field] = value
        out.append(r2)
    return out


def build_board_snapshot(mac=None, *, board_types: tuple[int, ...] = (7, 8, 3)) -> list[dict]:
    """板块级行情快照（面板暂无消费方；可选增强 / 检查数据完整度用）。

    列: symbol(板块码.SH) / 板块代码 / 板块名称 / 层级 / 成分数 / 涨跌幅 / 成交额
    """
    own = False
    if mac is None:
        mac = _mac()
        own = True
    level_name = {7: "一级行业", 8: "二级行业", 9: "三级行业", 3: "概念", 4: "风格", 5: "地区"}
    rows: list[dict] = []
    try:
        for bt in board_types:
            for b in mac.get_board_list(bt):
                members = _members(mac, b.code)
                if not members:
                    continue
                changes: list[float] = []
                amount = 0.0
                up = down = 0
                for m in members:
                    close = float(m.fields.get("close", 0.0) or 0.0)
                    prev = float(m.fields.get("pre_close", 0.0) or 0.0)
                    amount += float(m.fields.get("amount", 0.0) or 0.0)
                    if prev > 0 and close > 0:
                        pct = (close / prev - 1.0) * 100.0
                        changes.append(pct)
                        if pct > 0:
                            up += 1
                        elif pct < 0:
                            down += 1
                rows.append(
                    {
                        "symbol": f"{b.code}.SH",
                        "板块代码": b.code,
                        "板块名称": b.name,
                        "层级": level_name.get(bt, str(bt)),
                        "成分数": len(members),
                        "涨跌幅": round(sum(changes) / len(changes), 4) if changes else None,
                        "成交额": amount,
                        "上涨家数": up,
                        "下跌家数": down,
                    }
                )
    finally:
        if own:
            try:
                mac.close()
            except Exception:  # noqa: BLE001
                pass
    return rows


def get_board_ranking(mac=None, *, level: int | str = 1, top_n: int = 30) -> dict:
    """板块排名。level: 1|2|3 → 申万一级/二级/三级; "concept" → 概念。"""
    from eltdx.mac.enums import BoardType

    mapping = {
        1: BoardType.YJ_LEVEL1,
        2: BoardType.YJ_LEVEL2,
        3: BoardType.YJ_LEVEL3,
        "concept": BoardType.GN,
        "style": BoardType.FG,
        "region": BoardType.DQ,
    }
    bt = mapping.get(level)
    if bt is None:
        raise ValueError(f"level 非法: {level}")
    own = False
    if mac is None:
        mac = _mac()
        own = True
    t0 = time.time()
    try:
        rows = mac.get_board_ranking(bt, top_n=top_n)
        return {
            "state": "ok",
            "level": level,
            "board_type": int(bt),
            "count": len(rows),
            "items": rows,
            "elapsed": round(time.time() - t0, 2),
            "source": "eltdx:mac.get_board_ranking",
        }
    finally:
        if own:
            try:
                mac.close()
            except Exception:  # noqa: BLE001
                pass


# ---------------------------------------------------------------------------
# 插件 provider 门面 (lead 指定的 A 部分契约)
# ---------------------------------------------------------------------------
CATEGORY_TO_LEVEL = {
    "industry": 1,
    "industry_l1": 1,
    "l1": 1,
    "一级行业": 1,
    "industry_l2": 2,
    "l2": 2,
    "二级行业": 2,
    "industry_l3": 3,
    "l3": 3,
    "三级行业": 3,
    "concept": "concept",
    "概念": "concept",
    "style": "style",
    "风格": "style",
    "region": "region",
    "地区": "region",
}


def get_industry_boards(
    category: str | int = "industry",
    date: str | None = None,
    *,
    top_n: int = 500,
    with_members: bool = True,
    mac=None,
) -> dict:
    """provider 门面: get_industry_boards(category, date)。

    category: "industry"(申万一级) | 1|2|3 | "concept" | "style" | "region"
    date:     仅做口径声明 —— 板块快照来自 MAC 实时行情, 不支持历史回放;
              省略即「当前快照」, 给非当日日期时返回 state=no_data 并说明。

    返回:
      {state, category, level, board_type, date, count, items:[{
          code, name, change_pct, amount, vol, main_net_amount,
          up_count, down_count, member_count, members:[...] }],
       elapsed, source}

    members 默认填充 (逐板块再拉一遍 get_board_members; 30 个一级板块约 +2~3s,
    345 个三级板块会到 +30s)。只想要行情时传 with_members=False。
    """
    import datetime as _dt

    if isinstance(category, int) and not isinstance(category, bool):
        if category not in (1, 2, 3):
            raise ValueError(f"category 整数必须为 1|2|3, 收到 {category!r}")
        level: int | str = category
    else:
        key = str(category).strip().lower()
        level = CATEGORY_TO_LEVEL.get(key)  # type: ignore[assignment]
        if level is None:
            raise ValueError(
                f"category 非法: {category!r} (可选 1|2|3 或 {sorted(CATEGORY_TO_LEVEL)})"
            )

    today = _dt.date.today().isoformat()
    want = str(date)[:10] if date else today
    if want != today:
        return {
            "state": "no_data",
            "message": (
                f"目标日 {want} 无法回放: 板块行情来自 MAC 实时快照 (当前 {today}); "
                "历史板块行情请用本地 parquet 快照"
            ),
            "category": category,
            "requested_date": want,
            "date": today,
        }

    ranking = get_board_ranking(mac=mac, level=level, top_n=top_n)

    own = False
    if with_members and mac is None:
        mac = _mac()
        own = True
    try:
        items = []
        for r in ranking["items"]:
            members: list[str] = []
            if with_members:
                members = [_thscode(m.market, m.code) for m in _members(mac, str(r.get("code")))]
            items.append(
                {
                    "code": r.get("code"),
                    "name": r.get("name"),
                    "change_pct": r.get("change_pct"),
                    "amount": r.get("amount"),
                    "vol": r.get("vol"),
                    "main_net_amount": r.get("main_net_amount"),
                    "up_count": r.get("up_count"),
                    "down_count": r.get("down_count"),
                    "member_count": r.get("member_count") or len(members),
                    "members": members,
                }
            )
    finally:
        if own and mac is not None:
            try:
                mac.close()
            except Exception:  # noqa: BLE001
                pass
    return {
        "state": "ok",
        "category": category,
        "level": level,
        "board_type": ranking["board_type"],
        "date": today,
        "count": len(items),
        "items": items,
        "elapsed": ranking.get("elapsed"),
        "source": ranking.get("source"),
    }


def build_ext_rows(target: str = "industry", *, mac=None, separator: str | None = None) -> list[dict]:
    """产出可直接 POST /api/ext-data/{id}/ingest 的行业/概念行。

    target="industry" → 键 ['symbol','code','股票代码','股票简称','所属同花顺行业']
    target="concept"  → 键 ['symbol','code','股票代码','股票简称','所属概念']
    """
    t = str(target).lower()
    if t in ("industry", "行业", "hy"):
        return map_industry_fields(build_industry_rows(mac=mac), target_field="所属同花顺行业")
    if t in ("concept", "概念", "gn"):
        rows = build_concept_rows(mac=mac, separator=separator or ";")
        return rows
    raise ValueError(f"target 非法: {target!r} (可选 industry|concept)")



def main() -> int:
    import sys

    t0 = time.time()
    selftest = "--selftest" in sys.argv
    mac = _mac()
    try:
        t = time.time()
        ind = build_industry_tree(mac=mac)
        print(
            f"[boards] industry: l1={ind['counts']['l1']} l2={ind['counts']['l2']} "
            f"l3={ind['counts']['l3']} stocks={ind['counts']['stocks']} "
            f"skipped={ind['counts']['skipped']} elapsed={time.time()-t:.2f}s"
        )
        sample = list(ind["tree"].items())[:3]
        for (mk, code), path in sample:
            print(f"    {_thscode(mk, code)} {ind['names'].get((mk, code))} -> {'-'.join(path)}")

        t = time.time()
        con = build_concept_map(mac=mac)
        print(
            f"[boards] concept: boards={con['counts']['boards']} "
            f"stocks={con['counts']['stocks']} memberships={con['counts']['memberships']} "
            f"elapsed={time.time()-t:.2f}s"
        )
        for (mk, code), names in list(con["map"].items())[:2]:
            print(f"    {_thscode(mk, code)} {con['names'].get((mk, code))} -> {';'.join(names[:5])}")

        t = time.time()
        rows = build_industry_rows(mac=mac)
        print(f"[boards] industry_rows={len(rows)} {time.time()-t:.2f}s sample_keys={list(rows[0])}")

        for lv in (1, 2, 3, "concept"):
            t = time.time()
            r = get_board_ranking(mac=mac, level=lv, top_n=10)
            top = r["items"][0] if r["items"] else {}
            print(
                f"[boards] ranking level={lv} n={r['count']} {time.time()-t:.2f}s "
                f"top={top.get('name')} {top.get('change_pct')}%"
            )
        if selftest:
            t = time.time()
            ind_boards = get_industry_boards(category="industry", mac=mac)
            print(f"[boards] facade industry: state={ind_boards.get('state')} "
                  f"count={ind_boards.get('count')} {time.time()-t:.2f}s")
            if ind_boards.get("state") != "ok":
                return 1
            top = ind_boards["items"][0]
            print(f"    top={top['code']} {top['name']} {top['change_pct']}% "
                  f"amt={top['amount']/1e8:.1f}亿 members={len(top['members'])} "
                  f"main_net={top['main_net_amount']}")
            t = time.time()
            l2 = get_industry_boards(category=2, with_members=False, mac=mac)
            print(f"[boards] facade industry_l2(no-members): count={l2.get('count')} {time.time()-t:.2f}s")
            t = time.time()
            con = get_industry_boards(category="concept", with_members=False, top_n=10, mac=mac)
            print(f"[boards] facade concept(top10): count={con.get('count')} {time.time()-t:.2f}s")
            beyond = get_industry_boards(category="industry", date="2026-01-05", mac=mac)
            print(f"[boards] out-of-window probe state={beyond.get('state')} msg={beyond.get('message')}")
            try:
                get_industry_boards(category="nope", mac=mac)
            except ValueError as e:
                print(f"[boards] category 校验 OK: {e}")
            t = time.time()
            ext_rows = build_ext_rows("industry", mac=mac)
            print(f"[boards] build_ext_rows('industry')={len(ext_rows)} keys={list(ext_rows[0])} "
                  f"{time.time()-t:.2f}s")
            print(f"    sample={ext_rows[0]}")
        print(f"[boards] TOTAL {time.time()-t0:.2f}s")
        return 0
    finally:
        try:
            mac.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    raise SystemExit(main())
