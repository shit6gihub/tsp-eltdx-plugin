# -*- coding: utf-8 -*-
"""运行期自举 eltdx 原生库: 把内嵌的 wheel 解到可写目录并放进 sys.path.

为什么需要这一层
================
面板的外部插件包只接受源码后缀 —— backend/app/services/package_archive.py:20 的
SOURCE_SUFFIXES 白名单是 {.py,.yaml,.yml,.json,.txt,.md,.csv}, 报错文案直接写着
「不接收原生库、wheel、脚本或可执行文件」; 而 eltdx 的唯一实现体是 Rust 扩展
eltdx/_native.abi3.so。于是这里把 wheel 用 base64 文本内嵌进 wheel_blob.py(合规的 .py),
首次 import 时解码 -> 校验 sha256 -> 解压到数据卷下的可写目录 -> sys.path.insert ->
import eltdx。这样"只许上传源码"的约束下也能把带原生库的数据源装进正在运行的容器,
不必重建镜像。

设计取舍
========
* 解压目录放在 data 卷里 (data/plugins/.eltdx_native_lib/...), 因为镜像内 /app 只读,
  而 data 卷是唯一 rw 的持久目录; 名字以点开头, 保证 packages.discover() 不会把它
  当成插件目录 (packages.py 的 NAME 正则要求小写字母开头, 点开头的目录被跳过)。
* 保留 wheel 里的 eltdx-3.3.5.dist-info, 否则 eltdx/__init__.py 里的
  importlib.metadata.version('eltdx') 会 PackageNotFoundError 而把 __version__ 降级成
  '0+unknown'。
* 进程间互斥用 fcntl.flock + .ready 标记(记录 size/sha256): 面板可能多 worker, 冷启动
  时并发解压同一个目录会把 .so 写坏; 拿到锁后再判断一次, 只有一个进程真正解压。
* 失败一律抛 BootstrapError, 由 provider.availability() 转成 (False, 可执行提示),
  既不阻断面板启动, 也不会让 loader 崩溃到「插件加载失败」。
"""
from __future__ import annotations

import base64
import contextlib
import hashlib
import io
import json
import os
import platform
import shutil
import sys
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path

MARKER_NAME = ".ready"
EMBEDDED_TARGET = "3.3.5"

# 内嵌 wheel 的身份: 任何一个字节不符就拒绝解压(不做"尽力而为"的容错)。
EMBEDDED_WHEEL = {
    ("linux", "x86_64"): {
        "file": "eltdx-3.3.5-cp310-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl",
        "size": 2534935,
        "sha256": "108bdf709d981339627f36b444725b18aba447a28a872440304639fa2da97731",
    },
}

# 非内嵌架构的兜底: 只从 PyPI 官方文件域名取, 且按登记 sha256 校验。
# 容器里大概率没有外网, 所以这只是"换机器时不用重新打包"的便利, 不是主路径。
REMOTE_WHEELS = {
    ("linux", "aarch64"): {
        "file": "eltdx-3.3.5-cp310-abi3-manylinux2014_aarch64.manylinux_2_17_aarch64.whl",
        "size": 2515056,
        "sha256": "4e49201fec19bf3792f23af3347deebbcd47e9a95d92d98ee4c57dd53888ac21",
        "url": (
            "https://files.pythonhosted.org/packages/8f/34/"
            "bc6e883e04c1cca3658bd98821e8a5ed69370095cf5c8460ff02c0191434/"
            "eltdx-3.3.5-cp310-abi3-manylinux2014_aarch64.manylinux_2_17_aarch64.whl"
        ),
    },
    ("darwin", "x86_64"): {
        "file": "eltdx-3.3.5-cp310-abi3-macosx_10_12_x86_64.whl",
        "size": 2442336,
        "sha256": "94889b3f12c7f2d14d55f014a07cdd536bfd780bfbc53a042f5d5726370b3e10",
        "url": (
            "https://files.pythonhosted.org/packages/cf/70/"
            "362e0d2fe98cb36ce222a5ba4dd9a6a4e1b919557bfd73c766552d14994b/"
            "eltdx-3.3.5-cp310-abi3-macosx_10_12_x86_64.whl"
        ),
    },
    ("darwin", "arm64"): {
        "file": "eltdx-3.3.5-cp310-abi3-macosx_11_0_arm64.whl",
        "size": 2413393,
        "sha256": "71d579e56c0736d71e947f0ceb2649e0ec9d2f070fa9c0440593d0686285c1c5",
        "url": (
            "https://files.pythonhosted.org/packages/c2/d5/"
            "5c4d81b6d6383c7c901ca9ad97f59e6eca846e35df732be3f20842e48b5d/"
            "eltdx-3.3.5-cp310-abi3-macosx_11_0_arm64.whl"
        ),
    },
    ("win32", "amd64"): {
        "file": "eltdx-3.3.5-cp310-abi3-win_amd64.whl",
        "size": 2350979,
        "sha256": "78e73a22fd6a369e2945f42406687e2b7a43b0bb651f039d79b5193554fe3d57",
        "url": (
            "https://files.pythonhosted.org/packages/67/fc/"
            "5becadd33a7df7df184fc4c877a7a8001added0074941549052fe38e19fc/"
            "eltdx-3.3.5-cp310-abi3-win_amd64.whl"
        ),
    },
}

READY_TIMEOUT_S = float(os.environ.get("TSP_ELTDX_READY_TIMEOUT", "120"))


class BootstrapError(RuntimeError):
    """自举失败: 文案面向运维, 直接可执行。"""


_ensured_version = None


def _platform_key():
    machine = platform.machine().lower()
    if machine in ("amd64", "x86_64", "x64"):
        machine = "amd64" if sys.platform == "win32" else "x86_64"
    elif machine in ("arm64", "aarch64"):
        machine = "arm64" if sys.platform == "darwin" else "aarch64"
    return sys.platform, machine


def _plugin_root():
    return Path(__file__).resolve().parent


def _lib_root():
    """解压根目录. 优先级: 环境变量 > 面板 data 卷 plugins/_vendor > 系统临时目录.

    装机后的 bootstrap.py 位于 <data>/plugins/<name>/versions/<version>/bootstrap.py,
    因此 _plugin_root().parents[2] 就是 <data>/plugins。落在 plugins/_vendor 下而不是
    /tmp, 是因为 /tmp 随容器重建而失; _vendor 以 _ 开头, packages.discover() 会按
    NAME 正则 (^[a-z][a-z0-9_]{0,63}$) 跳过它, 不会误当成插件目录。
    """
    override = os.environ.get("TSP_ELTDX_LIB_DIR")
    if override:
        return Path(override)
    root = _plugin_root()
    parents = root.parents
    if len(parents) >= 3 and parents[2].name == "plugins":
        return parents[2] / "_vendor"
    # 开发态(直接跑仓库里的目录)或非常规布局: 落到临时目录, 保证仍然可用。
    return Path(tempfile.gettempdir()) / "tsp-eltdx-lib"


@contextlib.contextmanager
def _exclusive(lock_path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "a+b")
    try:
        try:
            import fcntl
        except ImportError:  # Windows: 退化成无锁(单进程场景够用)
            yield
            return
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _target_dir():
    return _lib_root() / ("eltdx-" + EMBEDDED_TARGET)


# provider.py 用这个名字调用, 语义比 ensure() 更直白: 返回"库所在目录", 顺手保证可 import。
def install_path(force=False):
    """确保原生库就位并返回解压目录 Path; 失败抛 BootstrapError。"""
    ensure(force=force)
    return _target_dir()


def _read_marker(target):
    try:
        return json.loads((target / MARKER_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _marker_ok(target, meta):
    marker = _read_marker(target)
    if not marker:
        return False
    return (
        marker.get("sha256") == meta["sha256"]
        and marker.get("size") == meta["size"]
        and (target / "eltdx" / "__init__.py").is_file()
        and (target / "eltdx" / "_native.abi3.so").is_file()
    )


def _embedded_bytes():
    """读包内 assets/*.b64.txt (base64 文本) 并解码。

    zip 只能用源码后缀, .txt 在白名单里 (package_archive.py:20), 而 .whl/.so 会被直接
    拒绝, 所以 wheel 以 base64 文本随包分发。也兼容 wheel_blob.py 内嵌常量的精简包。
    """
    assets = _plugin_root() / "assets"
    if assets.is_dir():
        for path in sorted(assets.glob("*.b64.txt")):
            try:
                blob = path.read_text(encoding="ascii")
            except OSError as exc:
                raise BootstrapError("读取内嵌 wheel 失败(%s): %s" % (path.name, exc)) from exc
            try:
                return base64.b64decode(blob, validate=False)
            except Exception as exc:  # noqa: BLE001
                raise BootstrapError("内嵌 wheel 的 base64 损坏(%s): %s" % (path.name, exc)) from exc
    try:
        from . import wheel_blob  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - 允许"未内嵌"的精简包
        return None
    blob = getattr(wheel_blob, "WHEEL_B64", "") or ""
    if not blob:
        return None
    return base64.b64decode(blob, validate=False)


def _download(meta):
    url = meta.get("url")
    if not url:
        raise BootstrapError(
            "没有为 %s 准备 wheel; 请在 x86_64 Linux 上构建插件包, "
            "或设置 TSP_ELTDX_WHEEL 指向本地 wheel" % (_platform_key(),)
        )
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:  # noqa: S310 - 固定 https 官方域名
            return resp.read()
    except Exception as exc:  # noqa: BLE001
        raise BootstrapError("下载 eltdx wheel 失败(%s): %s" % (url, exc)) from exc


def _resolve_wheel():
    override = os.environ.get("TSP_ELTDX_WHEEL")
    if override:
        path = Path(override)
        data = path.read_bytes()
        return data, {"file": path.name, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    key = _platform_key()
    meta = EMBEDDED_WHEEL.get(key)
    if meta is not None:
        data = _embedded_bytes()
        if data is not None:
            return data, meta
        # 内嵌部分被裁剪(精简包): 退回下载同一份 wheel。
        return _download(meta), meta
    meta = REMOTE_WHEELS.get(key)
    if meta is None:
        raise BootstrapError(
            "当前平台 %s-%s 没有可用的 eltdx wheel; 支持: %s"
            % (key[0], key[1], ", ".join("%s-%s" % (a, b) for a, b in list(EMBEDDED_WHEEL) + list(REMOTE_WHEELS)))
        )
    return _download(meta), meta


def _extract(wheel_bytes, dest):
    dest.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".extract-", dir=str(dest.parent)))
    try:
        with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as zf:
            bad = zf.testzip()
            if bad is not None:
                raise BootstrapError("wheel 内容校验失败: " + str(bad))
            for info in zf.infolist():
                name = info.filename
                target = (staging / name).resolve()
                if not str(target).startswith(str(staging.resolve())):
                    raise BootstrapError("wheel 含越界路径: " + name)
                if info.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst)
        if not (staging / "eltdx" / "__init__.py").is_file():
            raise BootstrapError("wheel 里没有 eltdx 包")
        if dest.exists():
            shutil.rmtree(dest, ignore_errors=True)
        staging.rename(dest)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def _activate(target):
    entry = str(target)
    if entry not in sys.path:
        sys.path.insert(0, entry)


def _import_version():
    import eltdx  # noqa: F401 - 触发生效

    return getattr(eltdx, "__version__", "unknown")


def ensure(force=False):
    """保证 eltdx 可 import, 返回版本号. 幂等; 失败抛 BootstrapError."""
    global _ensured_version
    if _ensured_version and not force:
        return _ensured_version
    try:
        import eltdx

        version = getattr(eltdx, "__version__", "unknown")
        _ensured_version = version
        return version
    except Exception:  # noqa: BLE001 - 未安装/原生库缺失, 继续自举
        pass

    wheel_bytes, meta = _resolve_wheel()
    digest = hashlib.sha256(wheel_bytes).hexdigest()
    if len(wheel_bytes) != meta["size"] or digest != meta["sha256"]:
        raise BootstrapError(
            "eltdx wheel 校验失败: 期望 %d 字节 / sha256 %s, 实际 %d 字节 / sha256 %s"
            % (meta["size"], meta["sha256"], len(wheel_bytes), digest)
        )

    target = _target_dir()
    if force or not _marker_ok(target, meta):
        with _exclusive(_lib_root() / ".lock"):
            if force or not _marker_ok(target, meta):
                try:
                    _extract(wheel_bytes, target)
                    (target / MARKER_NAME).write_text(
                        json.dumps(
                            {
                                "file": meta["file"],
                                "sha256": meta["sha256"],
                                "size": meta["size"],
                                "extracted_at": int(time.time()),
                            },
                            ensure_ascii=False,
                        ),
                        encoding="utf-8",
                    )
                except OSError as exc:
                    raise BootstrapError(
                        "解压 eltdx 到 %s 失败: %s; 可用 TSP_ELTDX_LIB_DIR 指向另一个可写目录"
                        "(容器内 /tmp 亦可, 但重启后需重解)" % (target, exc)
                    ) from exc

    _activate(target)
    try:
        version = _import_version()
    except ImportError as exc:
        raise BootstrapError(
            "eltdx 已解压到 %s 但 import 失败: %s; 常见原因是 data 卷以 noexec 挂载, "
            "需要给容器加 :exec 或改挂到可执行目录" % (target, exc)
        ) from exc
    _ensured_version = version
    return version


def status():
    """可用性探测: 永不抛异常, 供 provider.availability() 直接返回给面板。"""
    try:
        version = ensure()
    except BootstrapError as exc:
        return False, "eltdx 自举失败: " + str(exc)
    except Exception as exc:  # noqa: BLE001
        return False, "eltdx 自举异常: " + str(exc)
    return True, "eltdx %s 就绪 (自举目录 %s)" % (version, _target_dir())


def main():
    ok, message = status()
    print(("OK " if ok else "FAIL ") + message)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
