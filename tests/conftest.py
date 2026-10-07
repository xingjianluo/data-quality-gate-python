"""pytest 公共夹具：每个用例独立的 JSONL 留痕文件。"""

from __future__ import annotations

import pytest

from dqgate.store import ResultStore


@pytest.fixture()
def store(tmp_path):
    return ResultStore(tmp_path / "results.jsonl")


@pytest.fixture()
def store_path(tmp_path):
    return str(tmp_path / "results.jsonl")
