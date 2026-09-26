"""本地数据集加载与指纹计算。"""
from __future__ import annotations

import hashlib
import json
import os
from typing import Any

DATA_DIR = os.environ.get("DQ_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))


def load_dataset(dataset_id: str) -> list[dict[str, Any]]:
    """从本地 data/ 目录加载 JSON 数据集（list[dict]）。

    文件不存在时抛 FileNotFoundError，绝不静默返回空数据。
    """
    path = os.path.join(DATA_DIR, f"{dataset_id}.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"dataset not found: {dataset_id} ({path})")
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise ValueError(f"dataset {dataset_id} must be a JSON array of objects")
    return rows


def fingerprint(rows: list[dict[str, Any]]) -> str:
    """计算数据集内容指纹：规范化 JSON 后取 SHA-256，保证同数据同指纹。"""
    canonical = json.dumps(rows, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
