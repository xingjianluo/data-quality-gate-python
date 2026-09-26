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
def results(dataset_id: str | None = None, rule_id: str | None = None, rule_version: int | None = None):
    """按数据集/规则/版本查询历史结论（不同版本可区分）。"""
    return {"results": _store.query(dataset_id, rule_id, rule_version)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
