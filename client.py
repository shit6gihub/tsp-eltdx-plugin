"""eltdx 客户端封装: 单例连接池 + 标的代码互转.

代码约定 (与面板一致):
- 标准码   "600519.SH" / "000001.SZ" / "830799.BJ"  (面板/DB 用这个)
- 通达信码 "sh600519" / "sz000001" / "bj830799"    (eltdx 用这个)
- 裸码     "600519"

裸码交易所推断规则: 6 开头 -> sh; 0/3 开头 -> sz; 4/8 开头 -> bj;
补充 B 股: 9 开头 -> sh, 2 开头 -> sz。
"""
from __future__ import annotations

import threading

POOL_SIZE = 8

_EX_PREFIXES = {"sh": "sh", "sz": "sz", "bj": "bj"}
_EX_SUFFIXES = {"sh": "SH", "sz": "SZ", "bj": "BJ"}

_lock = threading.Lock()
_client = None


def get_client():
    """进程内单例 TdxClient。首次调用建池 (pool_size=POOL_SIZE)。"""
    global _client
    if _client is not None:
        return _client
    with _lock:
        if _client is None:
            from eltdx import TdxClient

            _client = TdxClient.from_hosts(pool_size=POOL_SIZE)
    return _client


def close_client() -> None:
    """关闭单例连接池 (插件 close() 时调用)。失败忽略。"""
    global _client
    with _lock:
        cli, _client = _client, None
    if cli is None:
        return
    try:
        cli.close()
    except Exception:  # noqa: BLE001
        pass


def to_bare(code: str) -> str:
    """任意写法 -> 裸 6 位码。600519.SH / sh600519 / 600519 -> 600519。"""
    if code is None:
        return ""
    text = str(code).strip()
    if not text:
        return ""
    if "." in text:
        return text.split(".", 1)[0].strip()
    lowered = text.lower()
    if len(lowered) > 6 and lowered[:2] in _EX_PREFIXES:
        return text[2:]
    return text


def infer_exchange(bare: str) -> str:
    """裸码 -> sh / sz / bj。"""
    code = to_bare(bare)
    if not code:
        return "sh"
    head = code[0]
    if head in ("6", "9"):
        return "sh"
    if head in ("0", "2", "3"):
        return "sz"
    if head in ("4", "8"):
        return "bj"
    if code[:2] in ("43", "83", "87", "88", "92"):
        return "bj"
    return "sh"


def split_code(code: str) -> tuple[str, str]:
    """任意写法 -> (裸码, sh|sz|bj)。"""
    text = "" if code is None else str(code).strip()
    if not text:
        return "", "sh"
    if "." in text:
        bare, _, suffix = text.partition(".")
        prefix = suffix.strip().lower()
        if prefix in _EX_PREFIXES:
            return bare.strip(), prefix
        return bare.strip(), infer_exchange(bare)
    lowered = text.lower()
    if len(lowered) > 6 and lowered[:2] in _EX_PREFIXES:
        return text[2:], lowered[:2]
    return text, infer_exchange(text)


def to_tdx(symbol: str) -> str:
    """标准码/裸码 -> eltdx 码。600519.SH -> sh600519。"""
    bare, prefix = split_code(symbol)
    return prefix + bare if bare else ""


def to_std(code: str) -> str:
    """eltdx 码/裸码 -> 标准码。sh600519 -> 600519.SH。"""
    bare, prefix = split_code(code)
    return f"{bare}.{_EX_SUFFIXES.get(prefix, 'SH')}" if bare else ""


def exchange_name(code: str) -> str:
    """任意写法 -> SH / SZ / BJ。"""
    _, prefix = split_code(code)
    return _EX_SUFFIXES.get(prefix, "SH")


def tdx_codes(symbols) -> list[str]:
    """批量转换成 eltdx 码并去重(保序)。"""
    seen: set[str] = set()
    out: list[str] = []
    for symbol in symbols or []:
        code = to_tdx(symbol)
        if code and code not in seen:
            seen.add(code)
            out.append(code)
    return out
