"""eltdx 缓存预热后台线程 — 供 package/provider.py 的 WarmerLoop 引用。

依赖的 gaps 模块放在 plugin/warmer/ 子目录 (dragon_tiger.py / auction.py)。
"""
from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
import traceback
from datetime import date as _date_cls, datetime
from pathlib import Path
from typing import Optional

LOG = logging.getLogger(__name__)


class WarmerLoop:
    """缓存预热后台线程 (daemon, 可中断)。

    接线 (provider 侧最小改动):
        # __init__ 末尾
        self._warmer = WarmerLoop(data_dir, gaps_dir=str(Path(__file__).resolve().parent / "warmer"))
        self._warmer.start()
        # close() 里
        if getattr(self, "_warmer", None) is not None:
            self._warmer.stop(timeout=5.0)
    """

    def __init__(self, data_dir: str | Path, *, gaps_dir: str | None = None,
                 days: int = 3, interval_s: float = 1800.0,
                 initial_delay_s: float = 60.0, auction: bool = True) -> None:
        self.data_dir = Path(data_dir)
        self.gaps_dir = gaps_dir
        self.days = days
        self.interval_s = float(interval_s)
        self.initial_delay_s = float(initial_delay_s)
        self.auction = auction
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="tsp-eltdx-cache-warmer", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout)
        self._thread = None

    def is_alive(self) -> bool:
        return bool(self._thread is not None and self._thread.is_alive())

    def _run(self) -> None:
        if self._stop.wait(self.initial_delay_s):
            return
        while not self._stop.is_set():
            try:
                self._tick_once()
            except Exception:  # noqa: BLE001
                LOG.warning("cache-warmer 单轮异常 (线程继续):\n%s", traceback.format_exc())
            if self._stop.wait(self.interval_s):
                return

    def _tick_once(self) -> None:
        today = datetime.now().date()
        if self.gaps_dir and self.gaps_dir not in sys.path:
            sys.path.insert(0, self.gaps_dir)

        # ---- 龙虎榜: 窗口探测 + 写 < today 的历史日 ----
        try:
            import importlib
            dt_mod = importlib.import_module("dragon_tiger")
            probe = dt_mod.build_dragon_tiger_payload()
            if probe.get("state") == "ok":
                window = [str(x)[:10] for x in (probe.get("window") or [probe["trade_date"]])]
                for iso in window:
                    d = _date_cls.fromisoformat(iso)
                    if d >= today:
                        continue
                    path = self.data_dir / "dragon_tiger" / f"date={d.isoformat()}.json"
                    cached = self._read_state(path)
                    if cached == "ok":
                        continue
                    payload = dt_mod.build_dragon_tiger_payload(target=iso)
                    body = self._panel_dt_payload(payload, iso)
                    if body is None:
                        continue
                    self._write_atomic(path, body)
                    LOG.info("dragon_tiger %s: wrote %d items", iso, body["all"]["count"])
        except Exception:  # noqa: BLE001
            LOG.warning("dragon_tiger 预热异常:\n%s", traceback.format_exc())

        # ---- 竞价: 当日文件允许写 (次日生效); 历史日不重复跑 ----
        if self.auction:
            try:
                au_mod = importlib.import_module("auction")
                au_payload = au_mod.build_panel_cache_payload()
                body = self._panel_auction_payload(au_payload)
                if body is not None:
                    path = self.data_dir / "auction_benchmark" / f"date={today.isoformat()}.json"
                    cached = self._read_state(path)
                    if cached != "ok":
                        self._write_atomic(path, body)
                        LOG.info("auction_benchmark %s: wrote %d items (effective tomorrow)",
                                 today, body["count"])
            except Exception:  # noqa: BLE001
                LOG.warning("auction 预热异常:\n%s", traceback.format_exc())

    @staticmethod
    def _read_state(path: Path) -> str | None:
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            return doc.get("state") if isinstance(doc, dict) else None
        except OSError:
            return None

    @staticmethod
    def _write_atomic(path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".part")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        try:
            os.replace(tmp, path)
        except OSError:
            pass

    @staticmethod
    def _panel_dt_payload(payload: dict, requested: str | None) -> dict | None:
        if payload.get("state") != "ok":
            return None
        out = {
            "state": "ok",
            "requested_date": requested or payload.get("trade_date"),
            "trade_date": payload["trade_date"],
            "all": payload["all"],
            "org": payload["org"],
            "hot_money": payload["hot_money"],
        }
        for board in ("all", "org"):
            for item in out[board].get("stock_items") or []:
                item.setdefault("concept_list", [])
                item.setdefault("hot_rank", None)
                item.setdefault("limit_reason", None)
                item.setdefault("amount", None)
        return out

    @staticmethod
    def _panel_auction_payload(payload: dict) -> dict | None:
        if payload.get("state") != "ok":
            return None
        rows = payload.get("raw_items") or payload.get("item") or []
        items = [
            {"thscode": r.get("thscode"), "ticker": r.get("ticker"),
             "name": r.get("name"), "auction_pct": r.get("auction_pct"),
             "tags": r.get("tags") or []}
            for r in rows if isinstance(r, dict) and r.get("thscode")
        ]
        return {
            "state": "ok",
            "requested_date": payload.get("requested_date"),
            "trade_date": payload.get("trade_date"),
            "count": len(items),
            "raw_items": items,
        }
