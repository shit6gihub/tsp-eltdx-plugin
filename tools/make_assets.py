#!/usr/bin/env python3
"""重新生成 assets/eltdx-3.3.5.wheel.b64.txt（插件自举用的内嵌 wheel）。

为什么这个文件不进仓库（而是用脚本生成）：
  TSP 的插件包格式只放行 .py/.yaml/.json/.txt/.md/.csv，不接受原生库或 wheel，
  所以 bootstrap.py 需要一份 base64 文本才能在 linux-x86_64 上自举 eltdx。
  但它是 3.4MB 的生成物、且是纯可复现的转换（wheel → base64），
  没必要把二进制塞进 git 历史里。需要时跑一遍本脚本即可。

用法：
    python3 tools/make_assets.py                # 从 PyPI 下载并生成
    python3 tools/make_assets.py path/to/x.whl  # 用本地 wheel 生成

校验：
    下载后按 ELTDX_SHA256 校验，不符即报错退出，不会写入半成品。
    生成的 base64 与 v1.0.10 仓库外的原文件逐字节一致（200 字符/行）。
"""
from __future__ import annotations

import base64
import hashlib
import sys
import urllib.request
from pathlib import Path

NAME = "eltdx-3.3.5-cp310-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl"
SIZE = 2534935
ELTDX_SHA256 = "108bdf709d981339627f36b444725b18aba447a28a872440304639fa2da97731"
URL = (
    "https://files.pythonhosted.org/packages/a5/b7/"
    "4f3c5969793f5e96011643e35ca3748ee2a92f4db4357badcea5cecb422d/"
    + NAME
)
LINE = 200
OUT = Path(__file__).resolve().parent.parent / "assets" / "eltdx-3.3.5.wheel.b64.txt"


def load(argv: list[str]) -> bytes:
    if len(argv) > 1:
        blob = Path(argv[1]).read_bytes()
        print(f"本地 wheel: {argv[1]} ({len(blob)} bytes)")
        return blob
    print(f"下载 {NAME} ...")
    with urllib.request.urlopen(URL, timeout=120) as resp:  # noqa: S310
        blob = resp.read()
    print(f"已下载 {len(blob)} bytes")
    return blob


def main(argv: list[str]) -> int:
    blob = load(argv)
    if len(blob) != SIZE:
        print(f"FAIL: 大小不符，期望 {SIZE}，实际 {len(blob)}", file=sys.stderr)
        return 1
    digest = hashlib.sha256(blob).hexdigest()
    if digest != ELTDX_SHA256:
        print(f"FAIL: sha256 不符\n  期望 {ELTDX_SHA256}\n  实际 {digest}", file=sys.stderr)
        return 1

    b64 = base64.b64encode(blob).decode("ascii")
    lines = [b64[i : i + LINE] for i in range(0, len(b64), LINE)]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    # 与历史文件保持一致：每行 200 字符、行尾 \n、文件末也有 \n
    OUT.write_text("\n".join(lines) + "\n", encoding="ascii")
    print(f"OK: {OUT}  {OUT.stat().st_size} bytes / {len(lines)} 行")
    print(f"    sha256(wheel) = {digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
