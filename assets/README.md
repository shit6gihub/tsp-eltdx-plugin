# assets/

`eltdx-3.3.5.wheel.b64.txt` —— 插件自举用的内嵌 wheel（base64 文本）。

## 为什么需要它

TSP 插件包只放行 `.py/.yaml/.json/.txt/.md/.csv`，不接受原生库或 wheel。
而 `bootstrap.py` 要在 linux-x86_64 上自举 eltdx 原生库，所以必须有一份
base64 文本随包分发（用 `.txt` 后缀才合规）。

## 状态：已入库

本文件**已提交到仓库**，所以 `git clone` 后无需联网即可打包安装。

如果你需要重建或核验它（比如升级 eltdx 版本后），用生成脚本：

```bash
python3 tools/make_assets.py            # 从 PyPI 下载并校验 sha256 后生成
python3 tools/make_assets.py x.whl      # 或用本地已有的 wheel
```

脚本会校验 wheel 的大小与 sha256，不符直接失败，不会写出半成品。

## 校验标准

| 项 | 值 |
| --- | --- |
| wheel | `eltdx-3.3.5-cp310-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl` |
| size | 2534935 bytes |
| sha256 | `108bdf709d981339627f36b444725b18aba447a28a872440304639fa2da97731` |
| base64 折行 | 每行 200 字符，共 16900 行 / 3396816 bytes |
