"""并发校验执行器：并行执行、超时控制、部分失败、重跑与整轮墙钟时限。

终态定义（RunStatus）：
- PASSED：所有规则在所有分片上均 PASSED；
- FAILED：至少一条规则结论为 FAILED，且无任何 ERROR/REJECTED；
- PARTIAL：存在执行失败（ERROR，含超时/整轮时限到点）或被拒绝（REJECTED）
  的规则，整体结论不完整，绝不静默当作通过。

运行标识（run_id）：
- 默认由「数据集标识 + 数据内容指纹 + 规则集合指纹（类型/版本/参数）+ 分片数」
  确定性派生，与运行时刻无关；同一输入隔多久跑、重启后再跑，run_id 完全一致。

重跑语义（rerun_failed_only=True）：
- 同一数据内容版本下已有 PASSED 留痕的 (规则, 版本, 分片) 直接复用，
  不重复执行；数据内容变了（digest 不同）绝不复用别的内容版本的结论；
- FAILED / ERROR / REJECTED / 无留痕的部分重新执行；
- 由于结论按内容幂等写入，可复现的相同结论重复记录不会产生冲突。

整轮墙钟时限（run_deadline_sec）：
- 一次校验（含并发执行与结论收尾）的墙钟耗时硬上限，到点必须返回；
- 到点时仍未完成（含排队中、卡死、特别慢）的规则记为 ERROR
  「整轮时限到点，未得出结论」，绝不当通过，也不影响其他规则已得出的结论；
- rule_timeout_sec 仍是单条规则的软性超时，二者取更紧者生效。
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout, wait

from dqgate.dataset import Dataset, shard_dataset
from dqgate.identity import deterministic_run_id
from dqgate.models import CheckStatus, RuleDefinition, RuleResult, RunReport, RunStatus
from dqgate.rules import RuleRejectedError, execute_rule, validate_rule
from dqgate.store import ResultStore

log = logging.getLogger("dqgate.runner")

# 硬时限内为结论收尾（落盘留痕）预留的墙钟预算，保证"连收尾都算进去"也不超限
_FINALIZE_BUDGET_SEC = 0.05


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
        wall_start = time.monotonic()
        run_id = run_id or deterministic_run_id(
            dataset.dataset_id, dataset.digest, rules, self._num_shards)
        shards = shard_dataset(dataset, self._num_shards)

        reused: list[RuleResult] = []     # 复用的既有留痕（不重复执行）
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
                        dataset.dataset_id, rule.rule_id, rule.version, shard_name,
                        dataset_digest=dataset.digest)
                    if rec is not None and rec["status"] == CheckStatus.PASSED.value:
                        reused.append(RuleResult(
                            rule_id=rule.rule_id, rule_version=rule.version, shard=shard_name,
                            status=CheckStatus.PASSED, reason=rec["reason"],
                            evidence=rec["evidence"],
                        ))
                        continue
                tasks.append((rule, shard_name, shard_rows))

        executed = self._execute_parallel(tasks, wall_start)
        results = tuple(reused + immediate + executed)

        if self._store is not None:
            # 只写入本次新产生/复用的结论；幂等写保证可复现结论不冲突
            self._store.append(
                dataset.dataset_id, dataset.digest, list(results), run_id=run_id)

        elapsed = time.monotonic() - wall_start
        if self._run_deadline_sec is not None and elapsed > self._run_deadline_sec + 1e-9:
            # 收尾也计入墙钟预算；本地 JSONL 追加通常极快，超预算只告警不静默
            log.warning("整轮墙钟耗时 %.3fs 超过时限 %.3fs（收尾阶段）",
                        elapsed, self._run_deadline_sec)
        return RunReport(
            run_id=run_id,
            dataset_id=dataset.dataset_id,
            dataset_digest=dataset.digest,
            status=self._terminal_status(results),
            results=results,
        )

    def _execute_parallel(
        self,
        tasks: list[tuple[RuleDefinition, str, tuple[dict, ...]]],
        wall_start: float,
    ) -> list[RuleResult]:
        """线程池并发执行；单任务软超时与整轮硬时限双重控制，互不污染。

        - rule_timeout_sec：逐任务独立起算的软超时（从该任务被等待起计），
          超时只标记 ERROR，不中断工作线程，先去收集其他任务，不拖慢整轮；
        - run_deadline_sec：整轮墙钟硬上限（含收尾），到点时所有仍未得出
          结论的任务（卡死/排队/特别慢）统一记 ERROR 并立即返回。
        返回结果按任务提交顺序排序，结论顺序确定可复现。
        """
        if not tasks:
            return []

        hard_deadline = (
            wall_start + self._run_deadline_sec - _FINALIZE_BUDGET_SEC
            if self._run_deadline_sec else None
        )
        pool = ThreadPoolExecutor(max_workers=self._max_workers)
        try:
            # 一次性提交；排队中的任务在时限到点时可被 cancel
            ordered: list[tuple] = []
            for rule, shard, rows in tasks:
                fut = pool.submit(execute_rule, rule, rows, shard)
                ordered.append((fut, rule, shard))

            out_by_idx: dict[int, RuleResult] = {}
            timed_out: dict[int, tuple] = {}  # 已软超时、待硬截止时兜底的任务
            deadline_hit = False

            for idx, (fut, rule, shard) in enumerate(ordered):
                if fut.cancelled():
                    out_by_idx[idx] = self._error_result(
                        rule, shard, f"规则未被调度执行（整轮时限到点）")
                    continue
                # 该任务的等待窗口 = 单任务软超时 与 整轮剩余预算 的较小值
                per_task = self._rule_timeout_sec
                if hard_deadline is not None:
                    remain = hard_deadline - time.monotonic()
                    if remain <= 0:
                        deadline_hit = True
                        break
                    per_task = remain if per_task is None else min(per_task, remain)
                try:
                    out_by_idx[idx] = fut.result(timeout=per_task)
                except FutureTimeout:
                    if hard_deadline is not None and \
                            time.monotonic() >= hard_deadline - 1e-9:
                        deadline_hit = True
                        break
                    # 单任务软超时：先标记，继续收集后面的任务；
                    # 线程仍在后台跑，但本轮不再等待它
                    timed_out[idx] = (fut, rule, shard)
                    out_by_idx[idx] = self._error_result(
                        rule, shard,
                        f"规则执行超时（>{self._rule_timeout_sec}s）")
                except Exception as e:  # noqa: BLE001 - 执行失败必须可区分，不得静默
                    out_by_idx[idx] = self._error_result(
                        rule, shard, f"规则执行异常: {type(e).__name__}: {e}")

            if timed_out or deadline_hit:
                # 尝试在剩余预算内回收已经跑完的软超时任务；其余按硬时限处理
                pending_futs = {meta[0]: i for i, meta in timed_out.items()}
                if not deadline_hit and hard_deadline is not None and pending_futs:
                    salvage_wait = max(0.0, hard_deadline - time.monotonic())
                    done, _ = wait(set(pending_futs), timeout=salvage_wait)
                    for fut in done:
                        i = pending_futs.pop(fut)
                        _, rule, shard = timed_out[i]
                        try:
                            out_by_idx[i] = fut.result(timeout=0)
                        except Exception:  # noqa: BLE001 - 回收失败维持超时结论
                            pass
                stuck = list(timed_out.items())
                if deadline_hit:
                    log.warning("整轮墙钟时限到点（配置 %.3fs，已为收尾预留 %.3fs），"
                                "仍有规则未得出结论",
                                self._run_deadline_sec, _FINALIZE_BUDGET_SEC)
                    stuck.extend(
                        (i, meta) for i, meta in enumerate(ordered) if i not in out_by_idx)
                for i, (fut, rule, shard) in stuck:
                    fut.cancel()  # 仍在排队则直接取消；运行中的不强行打断
                    if deadline_hit:
                        out_by_idx[i] = self._error_result(
                            rule, shard,
                            f"整轮校验时限到点（>{self._run_deadline_sec}s），"
                            "规则未得出结论（不计为通过）")
                    # 非硬截止的普通软超时：out_by_idx 中已是"执行超时"结论，保持不变
        finally:
            # 不等待卡死线程收尾：到点必须返回；取消所有仍在排队的任务
            pool.shutdown(wait=False, cancel_futures=True)

        return [out_by_idx[i] for i in range(len(ordered))]

    @staticmethod
    def _error_result(rule: RuleDefinition, shard: str, reason: str) -> RuleResult:
        return RuleResult(
            rule_id=rule.rule_id, rule_version=rule.version, shard=shard,
            status=CheckStatus.ERROR, reason=reason,
        )

    @staticmethod
    def _terminal_status(results: tuple[RuleResult, ...]) -> RunStatus:
        statuses = {r.status for r in results}
        if statuses & {CheckStatus.ERROR, CheckStatus.REJECTED}:
            return RunStatus.PARTIAL
        if CheckStatus.FAILED in statuses:
            return RunStatus.FAILED
        return RunStatus.PASSED
