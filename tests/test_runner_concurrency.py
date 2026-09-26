"""并发执行：结果互不污染、超时映射为 ERROR、终态不静默通过。"""
import logging
import threading
import time

from dqgate.models import CheckStatus, RuleDefinition, RuleType, Verdict
from dqgate.runner import CheckRunner

logger = logging.getLogger("test.concurrency")


def test_concurrent_results_isolated_and_reproducible(base_rules):
    runner = CheckRunner(max_workers=5)
    rec1 = runner.run("orders", base_rules)
    rec2 = runner.run("orders", base_rules)
    from tests.conftest import log_run_summary
    log_run_summary(rec1, logger)
    # 同输入同结论、同 run_id（可复现），且每条规则结果独立
    assert rec1.run_id == rec2.run_id
    assert [(r.rule_id, r.status, r.reason) for r in rec1.results] == \
           [(r.rule_id, r.status, r.reason) for r in rec2.results]
    assert rec1.verdict is Verdict.FAILED  # 有 FAILED 无 ERROR -> FAILED
    statuses = {r.rule_id: r.status for r in rec1.results}
    assert statuses["qty-consistent"] is CheckStatus.PASSED
    assert statuses["amount-range"] is CheckStatus.FAILED


def test_timeout_becomes_error_and_verdict_partial(monkeypatch, base_rules):
    import dqgate.runner as runner_mod

    real_execute = runner_mod.execute_rule  # 捕获原始实现，避免补丁递归

    def slow_execute(rule, rows):
        if rule.rule_id == "amount-range":
            time.sleep(2)
        return real_execute(rule, rows)

    monkeypatch.setattr(runner_mod, "execute_rule", slow_execute)
    runner = CheckRunner(max_workers=5, timeout_seconds=0.3)
    rec = runner.run("orders", base_rules)
    from tests.conftest import log_run_summary
    log_run_summary(rec, logger)
    by_id = {r.rule_id: r for r in rec.results}
    assert by_id["amount-range"].status is CheckStatus.ERROR
    assert "timeout" in by_id["amount-range"].reason
    # 超时/错误不得让整体静默通过
    assert rec.verdict is Verdict.PARTIAL


def test_parallel_threads_do_not_share_mutable_state(base_rules):
    """同一 runner 被多线程同时调用，结果互不污染。"""
    runner = CheckRunner(max_workers=5)
    records, errors = [], []

    def work():
        try:
            records.append(runner.run("orders", base_rules))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(records) == 8
    assert {r.run_id for r in records} == {records[0].run_id}
    logger.info("8 concurrent runs all produced run_id=%s", records[0].run_id)
