"""本地数据质量校验与监控服务（FastAPI 入口）。"""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from dqgate.dataset import load_dataset
from dqgate.models import RuleDefinition, RuleType
from dqgate.monitor import diff_content, list_content_versions
from dqgate.runner import QualityRunner
from dqgate.store import ResultStore

STORE_PATH = os.environ.get("DQ_STORE_PATH", "dq_results.jsonl")

app = FastAPI(title="data-quality-gate")
_store = ResultStore(STORE_PATH)


class RuleIn(BaseModel):
    rule_id: str
    rule_type: RuleType
    version: int = Field(ge=1)
    params: dict[str, Any] = Field(default_factory=dict)


class ValidateIn(BaseModel):
    dataset_path: str
    rules: list[RuleIn]
    num_shards: int = Field(default=1, ge=1)
    max_workers: int = Field(default=4, ge=1)
    rule_timeout_sec: float = Field(default=10.0, gt=0)
    run_deadline_sec: float | None = Field(default=None, gt=0)
    rerun_failed_only: bool = False


@app.get("/")
def read_root():
    return {"service": "data-quality-gate", "store": STORE_PATH}


@app.post("/validate")
def validate(req: ValidateIn):
    """对本地数据集并发执行规则集合，返回终态报告并留痕。"""
    try:
        dataset = load_dataset(req.dataset_path)
    except (OSError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"数据集加载失败: {e}")
    runner = QualityRunner(
        store=_store,
        max_workers=req.max_workers,
        rule_timeout_sec=req.rule_timeout_sec,
        run_deadline_sec=req.run_deadline_sec,
        num_shards=req.num_shards,
    )
    report = runner.run(
        dataset,
        [RuleDefinition(r.rule_id, r.rule_type, r.version, r.params) for r in req.rules],
        rerun_failed_only=req.rerun_failed_only,
    )
    return {
        "run_id": report.run_id,
        "dataset_id": report.dataset_id,
        "dataset_digest": report.dataset_digest,
        "status": report.status.value,
        "results": [asdict(r) | {"status": r.status.value} for r in report.results],
    }


@app.get("/results")
def results(
    dataset_id: str | None = None,
    dataset_digest: str | None = None,
    rule_id: str | None = None,
    rule_version: int | None = None,
    run_id: str | None = None,
):
    """按数据集/数据内容版本/规则/规则版本/运行查询历史结论。"""
    return {
        "results": _store.query(
            dataset_id=dataset_id,
            dataset_digest=dataset_digest,
            rule_id=rule_id,
            rule_version=rule_version,
            run_id=run_id,
        )
    }


@app.get("/datasets/{dataset_id}/content-versions")
def content_versions(dataset_id: str):
    """列出数据集历史上跑过的所有数据内容版本及各版本结论计数。"""
    return {"dataset_id": dataset_id, "versions": list_content_versions(_store, dataset_id)}


class DiffIn(BaseModel):
    old_digest: str
    new_digest: str
    rule_version: int | None = Field(default=None, ge=1)


@app.post("/datasets/{dataset_id}/diff")
def dataset_diff(dataset_id: str, req: DiffIn):
    """对比同一数据集、同一规则版本下前后两个数据内容版本的违规差异。"""
    try:
        diff = diff_content(
            _store, dataset_id, req.old_digest, req.new_digest, req.rule_version)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {
        "dataset_id": diff.dataset_id,
        "rule_version": diff.rule_version,
        "old_digest": diff.old_digest,
        "new_digest": diff.new_digest,
        "counts": {
            "new": diff.new_count,
            "fixed": diff.fixed_count,
            "unresolved": diff.unresolved_count,
        },
        "new": [asdict(i) | {"kind": i.kind.value} for i in diff.new_items],
        "fixed": [asdict(i) | {"kind": i.kind.value} for i in diff.fixed_items],
        "unresolved": [asdict(i) | {"kind": i.kind.value} for i in diff.unresolved_items],
    }


@app.get("/runs")
def runs(dataset_id: str | None = None):
    """按运行标识列出历史运行摘要（运行标识对相同输入可复现）。"""
    return {"runs": _store.query_runs(dataset_id)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
