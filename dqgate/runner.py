"""并发校验执行器：并行执行、超时控制、部分失败与只重跑失败部分。

终态语义（绝不静默判通过）：
- 任一规则 ERROR / REJECTED -> 整体 PARTIAL（未完整评估，不能下通过/失败结论）
- 否则任一规则 FAILED      -> 整体 FAILED
- 全部 PASSED              -> 整体 PASSED

结果按 rule_id 排序，run_id 为输入内容哈希，保证同输入同记录（可复现）。
"""
from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from .dataset import fingerprint, load_dataset
from .models import CheckResult, CheckStatus, RuleDefinition, RunRecord, Verdict
from .rules import RuleValidationError, execute_rule


def _run_id(dataset_fp: str, specs: list[dict]) -> str:
    payload = json.dumps({"fp": dataset_fp, "rules": specs}, sort_keys=True)
    return "run-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _rule_spec(r: RuleDefinition) -> dict:
    return {"rule_id": r.rule_id, "type": r.rule_type.value,
            "version": r.version, "params": r.params}


class CheckRunner:
    """对同一数据集并发执行多条规则。"""

    def __init__(self, max_workers: int = 4, timeout_seconds: float = 10.0) -> None:
        self.max_workers = max_workers
        self.timeout_seconds = timeout_seconds

    def _execute_one(self, rule: RuleDefinition, rows, ds_id: str, ds_fp: str) -> CheckResult:
        """执行单条规则，把非法定义与执行异常映射为可区分的状态。"""
        try:
            res = execute_rule(rule, rows)
        except RuleValidationError as e:
            return CheckResult(rule.rule_id, rule.version, ds_id, ds_fp,
                               CheckStatus.REJECTED, f"invalid rule definition: {e}")
        except Exception as e:  # noqa: BLE001 - 执行失败必须显式留痕，不得静默
            return CheckResult(rule.rule_id, rule.version, ds_id, ds_fp,
                               CheckStatus.ERROR, f"execution error: {type(e).__name__}: {e}")
        return CheckResult(rule.rule_id, rule.version, ds_id, ds_fp, res.status,
                           res.reason, res.evidence)

    def run(self, dataset_id: str, rules: list[RuleDefinition],
            only_rule_ids: set[str] | None = None,
            prior: RunRecord | None = None) -> RunRecord:
        """并发执行校验。

        only_rule_ids 提供时，仅重跑集合内的规则；其余规则结论直接复用
        prior（必须为同一数据集指纹的历史运行），不重复执行已成功部分。
        """
        rows = load_dataset(dataset_id)
        ds_fp = fingerprint(rows)

        reuse: dict[str, CheckResult] = {}
        to_run = list(rules)
        if only_rule_ids is not None:
            if prior is None or prior.dataset_fingerprint != ds_fp:
                raise ValueError("partial rerun requires prior run with same dataset fingerprint")
            prior_by_id = {r.rule_id: r for r in prior.results}
            to_run = []
            for rule in rules:
                if rule.rule_id in only_rule_ids:
                    to_run.append(rule)
                else:
                    prev = prior_by_id.get(rule.rule_id)
                    if prev is None:
                        raise ValueError(f"no prior result for rule {rule.rule_id}")
                    reuse[rule.rule_id] = prev

        results: list[CheckResult] = list(reuse.values())
        pool = ThreadPoolExecutor(max_workers=max(1, self.max_workers))
        try:
            futures = {pool.submit(self._execute_one, rule, rows, dataset_id, ds_fp): rule
                       for rule in to_run}
            for fut, rule in futures.items():
                try:
                    results.append(fut.result(timeout=self.timeout_seconds))
                except FutureTimeout:
                    results.append(CheckResult(
                        rule.rule_id, rule.version, dataset_id, ds_fp,
                        CheckStatus.ERROR,
                        f"execution timeout after {self.timeout_seconds}s"))
        finally:
            # 不等已超时任务：超时结论已立即落定，运行不得被挂住
            pool.shutdown(wait=False, cancel_futures=True)

        results.sort(key=lambda r: (r.rule_id, r.rule_version))
        verdict = self._verdict(results)
        return RunRecord(
            run_id=_run_id(ds_fp, [_rule_spec(r) for r in
                                   sorted(rules, key=lambda x: (x.rule_id, x.version))]),
            dataset_id=dataset_id, dataset_fingerprint=ds_fp,
            verdict=verdict, results=tuple(results),
        )

    @staticmethod
    def _verdict(results: list[CheckResult]) -> Verdict:
        statuses = {r.status for r in results}
        if statuses & {CheckStatus.ERROR, CheckStatus.REJECTED}:
            return Verdict.PARTIAL
        if CheckStatus.FAILED in statuses:
            return Verdict.FAILED
        return Verdict.PASSED
