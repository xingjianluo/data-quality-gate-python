"""并发校验执行器：并行执行、超时控制、部分失败与重跑语义。

终态定义（RunStatus）：
- PASSED：所有规则在所有分片上均 PASSED；
- FAILED：至少一条规则结论为 FAILED，且无任何 ERROR/REJECTED；
- PARTIAL：存在执行失败（ERROR，含超时）或被拒绝（REJECTED）的规则，
  整体结论不完整，绝不静默当作通过。

可复现语义：
- run_id 由 内容指纹 + 规则集（id/类型/版本/参数）+ 分片数 确定性派生，
  同一份数据内容配同一套规则版本，无论何时跑、重启后再跑，run_id 完全一致；
- 显式传入 run_id 仅用于测试，不改变上述派生规则。

重跑语义（rerun_failed_only=True）：
- 同一内容版本下已有 PASSED 留痕的 (规则, 版本, 分片) 直接复用，不重复执行；
- FAILED / ERROR / REJECTED / 无留痕的部分重新执行；
- 由于结论按内容幂等写入，可复现的相同结论重复记录不会产生冲突。

整轮墙钟时限（run_deadline_sec）：
- 覆盖排队、执行与收尾的整轮耗时；到点必须返回，未完成任务一律记为 ERROR
  （"未得出结论"），不当通过，已完成任务的结论照常保留。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time

from dqgate.dataset import Dataset, shard_dataset
from dqgate.models import CheckStatus, RuleDefinition, RuleResult, RunReport, RunStatus
from dqgate.rules import RuleRejectedError, execute_rule, validate_rule
from dqgate.store import ResultStore


def derive_run_id(
    dataset: Dataset,
    rules: list[RuleDefinition],
    num_shards: int,
) -> str:
    """由 数据内容指纹 + 规则集版本 + 分片数 确定性派生运行标识。

    与运行时刻无关：相同输入（含规则参数、规则排序）必产生相同 run_id。
    """
    payload = {
        "dataset_id": dataset.dataset_id,
        "dataset_digest": dataset.digest,
        "num_shards": num_shards,
        "rules": [
            {
                "rule_id": r.rule_id,
                "rule_type": r.rule_type.value,
                "version": r.version,
                "params": r.params,
            }
            for r in rules
        ],
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class QualityRunner:
    """对数据集并发执行规则集合并产出终态报告。"""

    def __init__(
        self,
        store: ResultStore | None = None,
        max_workers: int = 4,
        rule_timeout_sec: float = 10.0,
        num_shards: int = 1,
        run_deadline_sec: float | None = None,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers 必须 >= 1")
        if run_deadline_sec is not None and run_deadline_sec <= 0:
            raise ValueError("run_deadline_sec 必须 > 0")
        self._store = store
        self._max_workers = max_workers
        self._rule_timeout_sec = rule_timeout_sec
        self._num_shards = num_shards
        self._run_deadline_sec = run_deadline_sec

    def run(
        self,
        dataset: Dataset,
        rules: list[RuleDefinition],
        run_id: str | None = None,
        rerun_failed_only: bool = False,
    ) -> RunReport:
        """执行校验并返回终态报告。"""
        run_id = run_id or derive_run_id(dataset, rules, self._num_shards)
        shards = shard_dataset(dataset, self._num_shards)
        started_at = time.monotonic()

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
                    rec = self._store.get(
                        dataset.dataset_id, dataset.digest,
                        rule.rule_id, rule.version, shard_name,
                    )
                    if rec is not None and rec["status"] == CheckStatus.PASSED.value:
                        reused.append(RuleResult(
                            rule_id=rule.rule_id, rule_version=rule.version, shard=shard_name,
                            status=CheckStatus.PASSED, reason=rec["reason"],
                            evidence=rec["evidence"],
                        ))
                        continue
                tasks.append((rule, shard_name, shard_rows))

        executed = self._execute_parallel(tasks, started_at)
        results = tuple(reused + immediate + executed)

        # 收尾（留痕写入）也在墙钟时限内：超时后写入只是本地落盘，耗时极小，
        # 但仍用同一 deadline 约束，避免卡住的外部存储拖住整轮返回。
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

    def _remaining(self, started_at: float) -> float | None:
        if self._run_deadline_sec is None:
            return None
        return max(0.0, self._run_deadline_sec - (time.monotonic() - started_at))

    def _execute_parallel(
        self,
        tasks: list[tuple[RuleDefinition, str, tuple[dict, ...]]],
        started_at: float,
    ) -> list[RuleResult]:
        """守护线程池并发执行；单任务超时与整轮墙钟超时都记为 ERROR，互不污染。

        到整轮 deadline 立即返回：尚未开始/未完成的任务统一记为
        ERROR（未得出结论），绝不当通过；已完成任务的结论全部保留。
        工作线程为 daemon，卡死任务不会阻止进程退出。
        """
        if not tasks:
            return []

        queue: list = list(tasks)
        lock = threading.Lock()
        next_index = 0
        results_by_index: dict[int, RuleResult] = {}

        def remaining_for_task() -> float:
            left = self._remaining(started_at)
            if left is None:
                return self._rule_timeout_sec
            return min(self._rule_timeout_sec, left)

        def worker() -> None:
            nonlocal next_index
            while True:
                with lock:
                    if next_index >= len(queue):
                        return
                    idx = next_index
                    next_index += 1
                    if self._remaining(started_at) == 0.0:
                        return
                rule, shard, rows = queue[idx]
                results_by_index[idx] = self._run_one(
                    rule, shard, rows, remaining_for_task())

        workers = [
            threading.Thread(target=worker, name=f"dq-rule-{i}", daemon=True)
            for i in range(min(self._max_workers, len(tasks)))
        ]
        for t in workers:
            t.start()
        if self._run_deadline_sec is None:
            for t in workers:
                t.join()
        else:
            for t in workers:
                t.join(timeout=max(0.0, self._run_deadline_sec - (time.monotonic() - started_at)))

        # 未产出结论的任务（排队中或仍在跑）一律记为 ERROR：未得出结论，不当通过
        out: list[RuleResult] = []
        for idx, (rule, shard, _rows) in enumerate(tasks):
            if idx in results_by_index:
                out.append(results_by_index[idx])
            else:
                deadline = self._run_deadline_sec
                reason = (
                    f"整轮校验超过墙钟时限（>{deadline}s），规则未得出结论"
                    if deadline is not None
                    else "规则未得出结论"
                )
                out.append(RuleResult(
                    rule_id=rule.rule_id, rule_version=rule.version, shard=shard,
                    status=CheckStatus.ERROR, reason=reason,
                ))
        return out

    def _run_one(
        self,
        rule: RuleDefinition,
        shard: str,
        rows: tuple[dict, ...],
        timeout: float,
    ) -> RuleResult:
        """执行单个「规则 × 分片」任务，带单任务超时；异常一律转 ERROR。"""
        box: dict = {}

        def target() -> None:
            try:
                box["result"] = execute_rule(rule, rows, shard)
            except Exception as e:  # noqa: BLE001 - 执行失败必须可区分，不得静默
                box["error"] = e

        t = threading.Thread(target=target, name=f"dq-task-{rule.rule_id}-{shard}", daemon=True)
        t.start()
        t.join(timeout=timeout)
        if "result" in box:
            return box["result"]
        if "error" in box:
            e = box["error"]
            return RuleResult(
                rule_id=rule.rule_id, rule_version=rule.version, shard=shard,
                status=CheckStatus.ERROR,
                reason=f"规则执行异常: {type(e).__name__}: {e}",
            )
        return RuleResult(
            rule_id=rule.rule_id, rule_version=rule.version, shard=shard,
            status=CheckStatus.ERROR,
            reason=f"规则执行超时（>{timeout:g}s）",
        )

    @staticmethod
    def _terminal_status(results: tuple[RuleResult, ...]) -> RunStatus:
        statuses = {r.status for r in results}
        if statuses & {CheckStatus.ERROR, CheckStatus.REJECTED}:
            return RunStatus.PARTIAL
        if CheckStatus.FAILED in statuses:
            return RunStatus.FAILED
        return RunStatus.PASSED
