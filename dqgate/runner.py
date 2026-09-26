"""并发校验执行器：并行执行、超时控制、部分失败与重跑语义。

终态定义（RunStatus）：
- PASSED：所有规则在所有分片上均 PASSED；
- FAILED：至少一条规则结论为 FAILED，且无任何 ERROR/REJECTED；
- PARTIAL：存在执行失败（ERROR，含超时）或被拒绝（REJECTED）的规则，
  整体结论不完整，绝不静默当作通过。

重跑语义（rerun_failed_only=True）：
- 已有 PASSED 留痕的 (规则, 版本, 分片) 直接复用，不重复执行；
- FAILED / ERROR / REJECTED / 无留痕的部分重新执行；
- 由于结论按内容幂等写入，可复现的相同结论重复记录不会产生冲突。
"""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from dqgate.dataset import Dataset, shard_dataset
from dqgate.models import CheckStatus, RuleDefinition, RuleResult, RunReport, RunStatus
from dqgate.rules import RuleRejectedError, execute_rule, validate_rule
from dqgate.store import ResultStore


class QualityRunner:
    """对数据集并发执行规则集合并产出终态报告。"""

    def __init__(
        self,
        store: ResultStore | None = None,
        max_workers: int = 4,
        rule_timeout_sec: float = 10.0,
        num_shards: int = 1,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers 必须 >= 1")
        self._store = store
        self._max_workers = max_workers
        self._rule_timeout_sec = rule_timeout_sec
        self._num_shards = num_shards

    def run(
        self,
        dataset: Dataset,
        rules: list[RuleDefinition],
        run_id: str | None = None,
        rerun_failed_only: bool = False,
    ) -> RunReport:
        """执行校验并返回终态报告。"""
        run_id = run_id or uuid.uuid4().hex
        shards = shard_dataset(dataset, self._num_shards)

        reused: list[RuleResult] = []   # 复用的既有留痕（不重复执行）
        immediate: list[RuleResult] = []  # 无需执行即可确定的结果（如 REJECTED）
        tasks: list[tuple[RuleDefinition, str, tuple[dict, ...]]] = []

        for rule in rules:
            try:
                validate_rule(rule)
            except RuleRejectedError as e:
                # 定义非法：明确拒绝，不静默跳过、不默认通过
                immediate.append(RuleResult(
                    rule_id=rule.rule_id, rule_version=rule.version, shard="*",
                    status=CheckStatus.REJECTED, reason=f"规则定义非法: {e}",
                ))
                continue
            for shard_name, shard_rows in shards:
                if rerun_failed_only and self._store is not None:
                    rec = self._store.get(dataset.dataset_id, rule.rule_id, rule.version, shard_name)
                    if rec is not None and rec["status"] == CheckStatus.PASSED.value:
                        reused.append(RuleResult(
                            rule_id=rule.rule_id, rule_version=rule.version, shard=shard_name,
                            status=CheckStatus.PASSED, reason=rec["reason"],
                            evidence=rec["evidence"],
                        ))
                        continue
                tasks.append((rule, shard_name, shard_rows))

        executed = self._execute_parallel(tasks)
        results = tuple(reused + immediate + executed)

        if self._store is not None:
            # 只写入本次新产生/复用的结论；幂等写保证可复现结论不冲突
            self._store.append(dataset.dataset_id, dataset.digest, list(results))

        return RunReport(
            run_id=run_id,
            dataset_id=dataset.dataset_id,
            dataset_digest=dataset.digest,
            status=self._terminal_status(results),
            results=results,
        )

    def _execute_parallel(
        self, tasks: list[tuple[RuleDefinition, str, tuple[dict, ...]]]
    ) -> list[RuleResult]:
        """线程池并发执行，单任务超时或异常记为 ERROR，互不污染。"""
        out: list[RuleResult] = []
        if not tasks:
            return out
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {
                pool.submit(execute_rule, rule, rows, shard): (rule, shard)
                for rule, shard, rows in tasks
            }
            for fut, (rule, shard) in futures.items():
                try:
                    out.append(fut.result(timeout=self._rule_timeout_sec))
                except FutureTimeout:
                    out.append(RuleResult(
                        rule_id=rule.rule_id, rule_version=rule.version, shard=shard,
                        status=CheckStatus.ERROR,
                        reason=f"规则执行超时（>{self._rule_timeout_sec}s）",
                    ))
                except Exception as e:  # noqa: BLE001 - 执行失败必须可区分，不得静默
                    out.append(RuleResult(
                        rule_id=rule.rule_id, rule_version=rule.version, shard=shard,
                        status=CheckStatus.ERROR,
                        reason=f"规则执行异常: {type(e).__name__}: {e}",
                    ))
        return out

    @staticmethod
    def _terminal_status(results: tuple[RuleResult, ...]) -> RunStatus:
        statuses = {r.status for r in results}
        if statuses & {CheckStatus.ERROR, CheckStatus.REJECTED}:
            return RunStatus.PARTIAL
        if CheckStatus.FAILED in statuses:
            return RunStatus.FAILED
        return RunStatus.PASSED
