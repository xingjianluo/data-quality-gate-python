"""本地数据集加载与指纹计算（模拟上游输入）。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Dataset:
    """本地数据集：稳定标识 + 内容指纹 + 行记录。

    digest 只取决于行内容（键排序后的规范化 JSON），
    同一数据文件无论何时加载指纹都一致，保证结论可复现。
    """

    dataset_id: str
    digest: str
    rows: tuple[dict[str, Any], ...]


def _canonical(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def compute_digest(rows: list[dict[str, Any]]) -> str:
    """对行集合计算稳定内容指纹。"""
    return hashlib.sha256(_canonical(list(rows)).encode("utf-8")).hexdigest()


def load_dataset(path: str | Path) -> Dataset:
    """从本地 JSON 文件加载数据集并计算内容指纹。

    文件格式：{"dataset_id": "...", "rows": [{...}, ...]}
    或纯数组 [{...}, ...]（此时以文件名作为 dataset_id）。
    """
    p = Path(path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        dataset_id = str(raw.get("dataset_id") or p.stem)
        rows = raw["rows"]
    elif isinstance(raw, list):
        dataset_id = p.stem
        rows = raw
    else:
        raise ValueError(f"不支持的数据集文件格式: {p}")
    if not all(isinstance(r, dict) for r in rows):
        raise ValueError("数据集的每一行必须是 JSON 对象")
    rows_t = tuple(rows)
    return Dataset(dataset_id=dataset_id, digest=compute_digest(list(rows_t)), rows=rows_t)


def shard_dataset(dataset: Dataset, num_shards: int) -> list[tuple[str, tuple[dict, ...]]]:
    """将数据集按行号取模稳定切分，返回 (分片名, 行) 列表。

    切分只取决于行顺序与分片数，结果确定可复现。
    """
    if num_shards < 1:
        raise ValueError("num_shards 必须 >= 1")
    buckets: list[list[dict]] = [[] for _ in range(num_shards)]
    for idx, row in enumerate(dataset.rows):
        buckets[idx % num_shards].append(row)
    return [(f"shard-{i}", tuple(b)) for i, b in enumerate(buckets)]
