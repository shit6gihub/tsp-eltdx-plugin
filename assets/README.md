# assets/

本目录的 `eltdx-3.3.5.wheel.b64.txt` **不随仓库提交**，用脚本生成。

## 为什么

TSP 插件包只放行 `.py/.yaml/.json/.txt/.md/.csv`，不接受原生库或 wheel。
而 `bootstrap.py` 需要在 linux-x86_64 上自举 eltdx 原生库，所以必须有一份
base64 文本随包分发（后缀 `.txt` 才合规）。

这个文件是 3.4 MB 的纯生成物，转换过程完全可复现，没必要进 git 历史。

## 生成

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
| base64 折行 | 每行 200 字符 |
