"""数据质量校验能力测试：并发执行、规则版本切换、部分失败与可复现性。

每个用例打印：输入摘要（数据集指纹/行数）、规则版本、判定依据。
"""

from __future__ import annotations

import logging
import time

import pytest

from dqgate.dataset import Dataset, compute_digest
from dqgate.models import CheckStatus, RuleDefinition, RuleType, RunStatus
from dqgate.runner import QualityRunner
from dqgate.store import DuplicateResultError, ResultStore

log = logging.getLogger("dqgate.test")


def make_dataset(rows, dataset_id="t-ds") -> Dataset:
    return Dataset(dataset_id=dataset_id, digest=compute_digest(list(rows)), rows=tuple(rows))


def log_report(dataset, rules, report) -> None:
    """单测日志：输入摘要、规则版本与判定依据。"""
    versions = {r.rule_id: r.version for r in rules}
    log.info("输入摘要 dataset=%s digest=%s rows=%d", dataset.dataset_id,
             dataset.digest[:12], len(dataset.rows))
    for r in report.results:
        log.info("规则=%s 版本=%s 分片=%s 结论=%s 判定依据=%s",
                 r.rule_id, versions.get(r.rule_id, r.rule_version), r.shard,
                 r.status.value, r.reason)
    log.info("终态=%s", report.status.value)


@pytest.fixture()
def store(tmp_path):
    return ResultStore(tmp_path / "results.jsonl")


# ---------- 规则类型覆盖与结论区分 ----------

def test_five_rule_types_pass_and_fail(store, caplog):
    caplog.set_level(logging.INFO)
    good = make_dataset([
        {"id": 1, "v": 10, "a": 1, "b": 2, "ts": 1000},
        {"id": 2, "v": 20, "a": 3, "b": 4, "ts": 1001},
    ], dataset_id="t-good")
    bad = make_dataset([
        {"id": 1, "v": 999, "a": 5, "b": 2, "ts": 100},
        {"id": 1, "a": 1, "b": 2},  # 缺 v/ts + 重复 id + 跨字段违例 + 过期
    ], dataset_id="t-bad")
    rules = [
        RuleDefinition("missing", RuleType.MISSING_FIELD, 1, {"field": "v"}),
        RuleDefinition("range", RuleType.VALUE_RANGE, 1, {"field": "v", "min": 0, "max": 100}),
        RuleDefinition("dup", RuleType.DUPLICATE_RECORD, 1, {"keys": ["id"]}),
        RuleDefinition("fresh", RuleType.FRESHNESS, 1,
                       {"field": "ts", "max_age_seconds": 60, "now": 1030}),
        RuleDefinition("xfield", RuleType.CROSS_FIELD, 1, {"left": "a", "right": "b", "op": "le"}),
    ]
    runner = QualityRunner(store=store)
    rep_good = runner.run(good, rules)
    log_report(good, rules, rep_good)
    assert rep_good.status == RunStatus.PASSED
    assert all(r.status == CheckStatus.PASSED for r in rep_good.results)

    rep_bad = runner.run(bad, rules)
    log_report(bad, rules, rep_bad)
    assert rep_bad.status == RunStatus.FAILED  # 数据问题 -> FAILED 而非 PARTIAL
    assert all(r.status == CheckStatus.FAILED for r in rep_bad.results)
    assert all(r.reason for r in rep_bad.results)  # 每条失败都有可解释依据


def test_invalid_rule_definition_rejected_not_skipped(store, caplog):
    caplog.set_level(logging.INFO)
    ds = make_dataset([{"v": 1}])
    rules = [
        RuleDefinition("ok", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 10}),
        RuleDefinition("no-params", RuleType.VALUE_RANGE, 1, {"field": "v"}),      # 缺 min/max
        RuleDefinition("bad-op", RuleType.CROSS_FIELD, 1,
                       {"left": "a", "right": "b", "op": "??"}),                    # 非法操作符
        RuleDefinition("bad-version", RuleType.MISSING_FIELD, 0, {"field": "v"}),   # 非法版本
    ]
    rep = QualityRunner(store=store).run(ds, rules)
    log_report(ds, rules, rep)
    by_id = {r.rule_id: r for r in rep.results}
    assert by_id["ok"].status == CheckStatus.PASSED
    for rid in ("no-params", "bad-op", "bad-version"):
        assert by_id[rid].status == CheckStatus.REJECTED
        assert "规则定义非法" in by_id[rid].reason
        assert by_id[rid].reason.count("规则定义非法") == 1
    # 三条拒绝原因可区分
    reasons = {by_id[r].reason for r in ("no-params", "bad-op", "bad-version")}
    assert len(reasons) == 3
    assert rep.status == RunStatus.PARTIAL  # 被拒绝不得静默当作通过


# ---------- 并发执行与部分失败 ----------

def test_concurrent_shards_isolated_and_partial_failure(store, caplog, monkeypatch):
    caplog.set_level(logging.INFO)
    rows = [{"id": i, "v": i} for i in range(40)]
    ds = make_dataset(rows)
    rules = [
        RuleDefinition("missing", RuleType.MISSING_FIELD, 1, {"field": "v"}),
        RuleDefinition("range", RuleType.VALUE_RANGE, 1, {"field": "v", "min": 0, "max": 100}),
        RuleDefinition("dup", RuleType.DUPLICATE_RECORD, 1, {"keys": ["id"]}),
        RuleDefinition("boom", RuleType.FRESHNESS, 1,
                       {"field": "v", "max_age_seconds": 1, "now": 0}),
    ]

    import dqgate.runner as runner_mod
    real_execute = runner_mod.execute_rule

    def flaky_execute(rule, shard_rows, shard):
        if rule.rule_id == "boom" and shard == "shard-1":
            raise RuntimeError("模拟上游解析失败")
        return real_execute(rule, shard_rows, shard)

    monkeypatch.setattr(runner_mod, "execute_rule", flaky_execute)
    runner = QualityRunner(store=store, max_workers=8, num_shards=4)
    rep = runner.run(ds, rules)
    log_report(ds, rules, rep)

    assert rep.status == RunStatus.PARTIAL  # 部分失败不得静默变为通过
    assert len(rep.results) == 4 * 4  # 4 规则 x 4 分片，结果互不污染、数量完整
    err = [r for r in rep.results if r.status == CheckStatus.ERROR]
    assert len(err) == 1 and err[0].rule_id == "boom" and err[0].shard == "shard-1"
    assert "模拟上游解析失败" in err[0].reason
    passed = [r for r in rep.results if r.status == CheckStatus.PASSED]
    assert len(passed) == 15  # 其余分片不受失败影响

    # 只重跑失败部分：已通过的 15 条复用，仅 shard-1 的 boom 重新执行
    monkeypatch.setattr(runner_mod, "execute_rule", real_execute)  # 故障恢复
    calls = []
    monkeypatch.setattr(runner_mod, "execute_rule",
                        lambda rule, sr, s: calls.append((rule.rule_id, s))
                        or real_execute(rule, sr, s))
    rep2 = runner.run(ds, rules, rerun_failed_only=True)
    log_report(ds, rules, rep2)
    assert calls == [("boom", "shard-1")]  # 只重跑失败部分
    assert rep2.status == RunStatus.PASSED  # 恢复后终态可收敛为通过
    # 历史留痕未被篡改：ERROR 记录仍在（按 run 追加的是新结论，同键同内容幂等）
    assert len(store.query("t-ds", "boom", 1)) == 4


def test_timeout_marked_error(store, caplog, monkeypatch):
    caplog.set_level(logging.INFO)
    ds = make_dataset([{"v": 1}])
    rules = [RuleDefinition("slow", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 10})]

    import dqgate.runner as runner_mod

    def slow_execute(rule, shard_rows, shard):
        time.sleep(2)
        raise AssertionError("不应执行完成")

    monkeypatch.setattr(runner_mod, "execute_rule", slow_execute)
    rep = QualityRunner(store=store, rule_timeout_sec=0.2).run(ds, rules)
    log_report(ds, rules, rep)
    assert rep.results[0].status == CheckStatus.ERROR
    assert "超时" in rep.results[0].reason
    assert rep.status == RunStatus.PARTIAL


# ---------- 规则版本化与留痕 ----------

def test_version_switch_preserves_history(store, caplog):
    caplog.set_level(logging.INFO)
    ds = make_dataset([{"id": 1, "v": 50}, {"id": 2, "v": 150}])
    runner = QualityRunner(store=store)
    v1 = [RuleDefinition("range", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 200})]
    v2 = [RuleDefinition("range", RuleType.VALUE_RANGE, 2, {"field": "v", "max": 100})]

    rep1 = runner.run(ds, v1)
    log_report(ds, v1, rep1)
    assert rep1.status == RunStatus.PASSED

    rep2 = runner.run(ds, v2)  # 规则升级后重新校验
    log_report(ds, v2, rep2)
    assert rep2.status == RunStatus.FAILED

    hist_v1 = store.query("t-ds", "range", 1)
    hist_v2 = store.query("t-ds", "range", 2)
    assert len(hist_v1) == 1 and hist_v1[0]["status"] == "passed"   # 旧结论未被篡改
    assert len(hist_v2) == 1 and hist_v2[0]["status"] == "failed"   # 新版本结论可区分
    assert hist_v1[0]["reason"] != hist_v2[0]["reason"]


def test_same_version_reproducible_and_no_overwrite(store, caplog):
    caplog.set_level(logging.INFO)
    ds = make_dataset([{"v": 5}])
    rules = [RuleDefinition("range", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 10})]
    runner = QualityRunner(store=store)
    rep1 = runner.run(ds, rules)
    rep2 = runner.run(ds, rules)  # 同数据集同版本重跑：结论可复现
    log_report(ds, rules, rep2)
    assert [(r.status, r.reason) for r in rep1.results] == [(r.status, r.reason) for r in rep2.results]
    assert len(store.query("t-ds", "range", 1)) == 1  # 幂等，不产生重复记录

    # 同键写入不一致结论被拒绝（防篡改）
    from dqgate.models import RuleResult
    with pytest.raises(DuplicateResultError):
        store.append("t-ds", ds.digest, [RuleResult(
            "range", 1, "shard-0", CheckStatus.FAILED, "伪造结论", {})])
