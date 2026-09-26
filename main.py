"""数据质量门禁服务：本地数据集 + 规则版本化校验 + 结论留痕查询。"""
from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dqgate.models import RuleDefinition, RuleType
from dqgate.runner import CheckRunner
from dqgate.store import ResultStore

STORE_PATH = os.environ.get("DQ_STORE_PATH", os.path.join(os.path.dirname(__file__), "data", "results.jsonl"))

app = FastAPI(title="data-quality-gate")
store = ResultStore(STORE_PATH)
runner = CheckRunner(max_workers=4, timeout_seconds=10.0)


class RuleIn(BaseModel):
    rule_id: str
    rule_type: RuleType
    version: int
    params: dict[str, Any] = {}


class RunRequest(BaseModel):
    dataset_id: str
    rules: list[RuleIn]
    only_rule_ids: list[str] | None = None  # 提供时只重跑这些规则，其余复用最近一次运行


def _record_to_dict(rec) -> dict:
    d = asdict(rec)
    d["verdict"] = rec.verdict.value
    for r, orig in zip(d["results"], rec.results):
        r["status"] = orig.status.value
    return d


@app.post("/checks/run")
def run_checks(req: RunRequest) -> dict:
    """执行（或部分重跑）校验并留痕。"""
    rules = [RuleDefinition(r.rule_id, r.rule_type, r.version, r.params) for r in req.rules]
    prior = None
    if req.only_rule_ids is not None:
        runs = [r for r in store._iter_runs() if r.dataset_id == req.dataset_id]
        if not runs:
            raise HTTPException(409, "partial rerun requires a prior run for this dataset")
        prior = runs[-1]
    try:
        rec = runner.run(req.dataset_id, rules,
                         only_rule_ids=set(req.only_rule_ids) if req.only_rule_ids else None,
                         prior=prior)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(409, str(e))
    store.append_run(rec)
    return _record_to_dict(rec)


@app.get("/checks/results")
def list_results(dataset_id: str, rule_id: str | None = None,
                 rule_version: int | None = None) -> list[dict]:
    """按数据集 / 规则 / 版本查询历史结论。"""
    out = []
    for r in store.list_results(dataset_id, rule_id, rule_version):
        d = asdict(r)
        d["status"] = r.status.value
        out.append(d)
    return out


@app.get("/")
def read_root() -> dict:
    return {"service": "data-quality-gate", "endpoints": ["/checks/run", "/checks/results"]}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
