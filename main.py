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
from dqgate.monitor import diff_stored
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
        num_shards=req.num_shards,
        run_deadline_sec=req.run_deadline_sec,
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
    rule_id: str | None = None,
    rule_version: int | None = None,
    dataset_digest: str | None = None,
    history: bool = False,
):
    """按数据集/内容版本/规则/版本查询历史结论（内容版本与规则版本都可区分）。

    - 传 dataset_digest：只查该数据内容版本的结论；
    - 不传：返回该数据集所有内容版本的结论，互不覆盖；
    - history=true：返回全量追加留痕（含被重跑取代的 error/rejected）。
    """
    fn = _store.query_history if history else _store.query
    return {"results": fn(dataset_id, rule_id, rule_version,
                          dataset_digest=dataset_digest)}


@app.get("/datasets/{dataset_id}/digests")
def list_digests(dataset_id: str):
    """列出某数据集已有留痕的全部数据内容版本指纹（监控选版本用）。"""
    return {"dataset_id": dataset_id, "digests": _store.list_digests(dataset_id)}


@app.get("/diff")
def diff(dataset_id: str, old_digest: str, new_digest: str):
    """对比同一数据集、同一套规则版本下两个内容版本的结论差异。

    返回 新出现/已修好/未解决/未得出结论 四类条数，以及按规则、按分片的下钻汇总。
    """
    d = diff_stored(_store, dataset_id, old_digest, new_digest)
    return {
        "dataset_id": d.dataset_id,
        "old_digest": d.old_digest,
        "new_digest": d.new_digest,
        "counts": d.counts,
        "counts_by_rule": d.counts_by_rule(),
        "counts_by_shard": d.counts_by_shard(),
        "new_violations": [c.__dict__ for c in d.new_violations],
        "fixed_violations": [c.__dict__ for c in d.fixed_violations],
        "unresolved_violations": [c.__dict__ for c in d.unresolved_violations],
        "inconclusive": [c.__dict__ for c in d.inconclusive],
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
