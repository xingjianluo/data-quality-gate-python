"""结论留痕存储：追加写、按版本查询、不覆盖历史。

存储为 JSONL（每行一个 RunRecord），只追加不修改，
因此历史结论永不被篡改或覆盖，且可按规则版本区分查询。
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict

from .models import CheckResult, CheckStatus, RunRecord, Verdict


def _result_to_dict(r: CheckResult) -> dict:
    d = asdict(r)
    d["status"] = r.status.value
    return d


def _result_from_dict(d: dict) -> CheckResult:
    return CheckResult(
        rule_id=d["rule_id"], rule_version=d["rule_version"],
        dataset_id=d["dataset_id"], dataset_fingerprint=d["dataset_fingerprint"],
        status=CheckStatus(d["status"]), reason=d["reason"],
        evidence=d.get("evidence", {}),
    )


class ResultStore:
    """基于本地 JSONL 的只追加存储（线程安全）。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    def append_run(self, record: RunRecord) -> None:
        """追加一次运行记录。只追加，绝不改写既有行。"""
        line = json.dumps({
            "run_id": record.run_id,
            "dataset_id": record.dataset_id,
            "dataset_fingerprint": record.dataset_fingerprint,
            "verdict": record.verdict.value,
            "results": [_result_to_dict(r) for r in record.results],
        }, ensure_ascii=False, sort_keys=True)
        with self._lock, open(self.path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def _iter_runs(self) -> list[RunRecord]:
        if not os.path.exists(self.path):
            return []
        runs = []
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                runs.append(RunRecord(
                    run_id=d["run_id"], dataset_id=d["dataset_id"],
                    dataset_fingerprint=d["dataset_fingerprint"],
                    verdict=Verdict(d["verdict"]),
                    results=tuple(_result_from_dict(r) for r in d["results"]),
                ))
        return runs

    def list_results(self, dataset_id: str, rule_id: str | None = None,
                     rule_version: int | None = None) -> list[CheckResult]:
        """按数据集 / 规则 / 规则版本查询历史结论。"""
        out: list[CheckResult] = []
        for run in self._iter_runs():
            if run.dataset_id != dataset_id:
                continue
            for r in run.results:
                if rule_id is not None and r.rule_id != rule_id:
                    continue
                if rule_version is not None and r.rule_version != rule_version:
                    continue
                out.append(r)
        return out
