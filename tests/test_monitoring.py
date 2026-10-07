"""持续监控能力测试：

业务覆盖：
1. 内容版本化留痕与可复现——同内容+同规则版本，隔次跑/重启后跑，
   结论与 run_id 完全一致；内容变化后新旧版本结论各自留痕、互不覆盖、可分别查。
2. 批次差异判定——同一数据集同一规则版本下，前后内容版本的
   新出现/已修好/未解决违规条数，可下钻到规则与分片；未得出结论单列不混淆。
3. 卡死规则下的整轮时限保证——墙钟耗时不超过上限，到点返回；
   卡住规则记 error（未得出结论，不当通过），其他规则结论不受影响。

每个用例打印：输入摘要（数据集/内容指纹/规则版本）、各规则分片结论、
差异条数或耗时，便于从日志看出覆盖了哪些业务。
"""

from __future__ import annotations

import logging
import time

import pytest

from dqgate.dataset import Dataset, compute_digest
from dqgate.models import CheckStatus, RuleDefinition, RuleType, RunStatus
from dqgate.monitor import BatchDiff, diff_results, diff_stored
from dqgate.runner import QualityRunner, derive_run_id
from dqgate.store import ResultStore

log = logging.getLogger("dqgate.monitor-test")


def make_dataset(rows, dataset_id="mon-ds") -> Dataset:
    return Dataset(dataset_id=dataset_id, digest=compute_digest(list(rows)), rows=tuple(rows))


def log_report(tag, dataset, rules, report, elapsed=None) -> None:
    versions = {r.rule_id: r.version for r in rules}
    log.info("[%s] dataset=%s digest=%s rows=%d%s", tag, dataset.dataset_id,
             dataset.digest[:12], len(dataset.rows),
             f" elapsed={elapsed:.2f}s" if elapsed is not None else "")
    for r in report.results:
        log.info("[%s] 规则=%s 版本=%s 分片=%s 结论=%s 依据=%s", tag,
                 r.rule_id, versions.get(r.rule_id, r.rule_version),
                 r.shard, r.status.value, r.reason)
    log.info("[%s] run_id=%s 终态=%s", tag, report.run_id[:12], report.status.value)


def log_diff(tag, diff: BatchDiff) -> None:
    log.info("[%s] 差异 %s -> %s 条数=%s", tag,
             diff.old_digest[:12], diff.new_digest[:12], diff.counts)
    for name, label in (
        ("new_violations", "新出现"),
        ("fixed_violations", "已修好"),
        ("unresolved_violations", "未解决"),
        ("inconclusive", "未得出结论"),
    ):
        for c in getattr(diff, name):
            log.info("[%s]   %s 规则=%s 版本=%s 分片=%s (%s -> %s)", tag, label,
                     c.rule_id, c.rule_version, c.shard, c.old_status, c.new_status)
    log.info("[%s] 按规则下钻=%s", tag, diff.counts_by_rule())
    log.info("[%s] 按分片下钻=%s", tag, diff.counts_by_shard())


@pytest.fixture()
def store(tmp_path):
    return ResultStore(tmp_path / "monitor_results.jsonl")


# ---------- 业务1：内容版本化留痕与可复现 ----------

def test_same_content_reproducible_across_reruns_and_restart(store, caplog, tmp_path):
    """同内容+同规则版本：隔次跑、换实例跑、重启加载后跑，run_id 与结论完全一致。"""
    caplog.set_level(logging.INFO)
    ds = make_dataset([{"id": 1, "v": 5}, {"id": 2, "v": 7}])
    rules = [
        RuleDefinition("range", RuleType.VALUE_RANGE, 1, {"field": "v", "min": 0, "max": 10}),
        RuleDefinition("missing", RuleType.MISSING_FIELD, 1, {"field": "v"}),
    ]
    runner = QualityRunner(store=store, num_shards=2)
    rep1 = runner.run(ds, rules)
    time.sleep(0.01)  # 模拟“隔了一段时间再跑”
    rep2 = runner.run(ds, rules)
    log_report("可复现-同实例", ds, rules, rep2)

    # 全新实例（模拟服务重启），重新加载同一份留痕文件
    reopened = ResultStore(store._path)
    rep3 = QualityRunner(store=reopened, num_shards=2).run(ds, rules)
    log_report("可复现-重启后", ds, rules, rep3)

    expected_run_id = derive_run_id(ds, rules, 2)
    assert rep1.run_id == expected_run_id
    assert rep2.run_id == rep1.run_id          # 与跑的时刻无关，不漂
    assert rep3.run_id == rep1.run_id          # 重启后仍一致
    sig = [(r.rule_id, r.shard, r.status.value, r.reason) for r in rep1.results]
    assert sig == [(r.rule_id, r.shard, r.status.value, r.reason) for r in rep3.results]
    # 幂等：同内容同版本不产生重复记录
    assert len(store.query("mon-ds", dataset_digest=ds.digest)) == len(rules) * 2


def test_changed_content_versions_coexist_and_queryable(store, caplog):
    """内容变化：新旧内容版本结论各自留痕、互不覆盖、不报错，可按内容版本分别查。"""
    caplog.set_level(logging.INFO)
    v1_rows = [{"id": 1, "v": 5}, {"id": 2, "v": 7}]
    v2_rows = [{"id": 1, "v": 5}, {"id": 2, "v": 999}]  # 第二行越界
    ds1 = make_dataset(v1_rows)
    ds2 = make_dataset(v2_rows)
    assert ds1.digest != ds2.digest
    rules = [RuleDefinition("range", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 10})]

    runner = QualityRunner(store=store)
    rep1 = runner.run(ds1, rules)
    rep2 = runner.run(ds2, rules)  # 同数据集、同规则版本、内容变了：不得报错、不得顶掉旧结论
    log_report("内容版本1", ds1, rules, rep1)
    log_report("内容版本2", ds2, rules, rep2)

    assert rep1.status == RunStatus.PASSED and rep2.status == RunStatus.FAILED
    assert rep1.run_id != rep2.run_id

    old = store.query("mon-ds", "range", 1, dataset_digest=ds1.digest)
    new = store.query("mon-ds", "range", 1, dataset_digest=ds2.digest)
    assert len(old) == 1 and old[0]["status"] == "passed"
    assert len(new) == 1 and new[0]["status"] == "failed"
    # 不带内容版本过滤时，两个版本的结论都在
    assert {r["dataset_digest"] for r in store.query("mon-ds")} == {ds1.digest, ds2.digest}
    assert store.list_digests("mon-ds") == [ds1.digest, ds2.digest]

    # 对旧内容再跑一次，旧结论仍原样保留且幂等
    rep1_again = runner.run(ds1, rules)
    assert rep1_again.status == RunStatus.PASSED
    assert len(store.query("mon-ds", "range", 1, dataset_digest=ds1.digest)) == 1


# ---------- 业务2：批次差异判定 ----------

def test_batch_diff_new_fixed_unresolved_counts_and_drilldown(caplog):
    """两批内容对比：新出现/已修好/未解决条数正确，且可下钻到规则与分片。"""
    caplog.set_level(logging.INFO)
    R = lambda rid, shard, status: __import__("dqgate.models", fromlist=["RuleResult"]).RuleResult(
        rid, 1, shard, CheckStatus(status), f"{rid}-{shard}-{status}", {})

    old = [
        R("range", "shard-0", "failed"),   # -> 新版本 passed：已修好
        R("range", "shard-1", "passed"),   # -> 新版本 failed：新出现
        R("dup", "shard-0", "failed"),     # -> 仍 failed：未解决
        R("dup", "shard-1", "passed"),     # -> 仍 passed：无差异，不出现在任何告警类
    ]
    new = [
        R("range", "shard-0", "passed"),
        R("range", "shard-1", "failed"),
        R("dup", "shard-0", "failed"),
        R("dup", "shard-1", "passed"),
    ]
    diff = diff_results("mon-ds", "digest-old", "digest-new", old, new)
    log_diff("差异判定", diff)

    assert diff.counts == {
        "new_violations": 1,
        "fixed_violations": 1,
        "unresolved_violations": 1,
        "inconclusive": 0,
    }
    n = diff.new_violations[0]
    assert (n.rule_id, n.shard, n.old_status, n.new_status) == (
        "range", "shard-1", "passed", "failed")
    assert (diff.fixed_violations[0].rule_id, diff.fixed_violations[0].shard) == (
        "range", "shard-0")
    assert (diff.unresolved_violations[0].rule_id,
            diff.unresolved_violations[0].shard) == ("dup", "shard-0")
    # 下钻：按规则
    by_rule = diff.counts_by_rule()
    assert by_rule["range"] == {
        "new_violations": 1, "fixed_violations": 1,
        "unresolved_violations": 0, "inconclusive": 0}
    assert by_rule["dup"]["unresolved_violations"] == 1
    # 下钻：按分片
    by_shard = diff.counts_by_shard()
    assert by_shard["shard-0"]["fixed_violations"] == 1
    assert by_shard["shard-0"]["unresolved_violations"] == 1
    assert by_shard["shard-1"]["new_violations"] == 1


def test_batch_diff_inconclusive_not_silently_passed(caplog):
    """任一侧 error/rejected（未得出结论）单列 inconclusive，绝不算修好也不算新违规。"""
    caplog.set_level(logging.INFO)
    from dqgate.models import RuleResult
    old = [RuleResult("slow", 1, "shard-0", CheckStatus.FAILED, "old", {})]
    new = [RuleResult("slow", 1, "shard-0", CheckStatus.ERROR, "上游卡死", {})]
    diff = diff_results("mon-ds", "a", "b", old, new)
    log_diff("未得出结论", diff)
    assert diff.counts["inconclusive"] == 1
    assert diff.counts["fixed_violations"] == 0
    assert diff.counts["new_violations"] == 0
    assert diff.inconclusive[0].new_status == "error"


def test_batch_diff_from_store_end_to_end(store, caplog):
    """端到端：跑两个内容版本，从留痕直接做差异对比。"""
    caplog.set_level(logging.INFO)
    rules = [
        RuleDefinition("range", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 10}),
        RuleDefinition("missing", RuleType.MISSING_FIELD, 1, {"field": "v"}),
    ]
    ds_old = make_dataset([{"id": 1, "v": 5}, {"id": 2, "v": 7}])
    ds_new = make_dataset([{"id": 1, "v": 5}, {"id": 2}])  # v 缺失 -> missing 失败；range 对缺失行失败
    runner = QualityRunner(store=store, num_shards=2)
    rep_old = runner.run(ds_old, rules)
    rep_new = runner.run(ds_new, rules)
    log_report("端到端-old", ds_old, rules, rep_old)
    log_report("端到端-new", ds_new, rules, rep_new)

    diff = diff_stored(store, "mon-ds", ds_old.digest, ds_new.digest)
    log_diff("端到端差异", diff)
    # 两条规则的 shard-1（第二行落在 idx%2=1）都从 passed -> failed = 新出现
    assert diff.counts["new_violations"] == 2
    assert {c.rule_id for c in diff.new_violations} == {"range", "missing"}
    assert all(c.shard == "shard-1" for c in diff.new_violations)
    assert diff.counts["fixed_violations"] == 0
    assert diff.counts["unresolved_violations"] == 0


# ---------- 业务3：卡死规则下的整轮时限保证 ----------

def test_run_returns_by_deadline_when_rule_hung(monkeypatch, caplog):
    """卡死规则：整轮墙钟不超过上限；到点返回，卡住规则记 error，不当通过。"""
    caplog.set_level(logging.INFO)
    import dqgate.runner as runner_mod

    def hung_execute(rule, rows, shard):
        time.sleep(30)  # 模拟上游卡死

    monkeypatch.setattr(runner_mod, "execute_rule", hung_execute)
    ds = make_dataset([{"v": 1}, {"v": 2}])
    rules = [RuleDefinition("hung", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 10})]

    runner = QualityRunner(max_workers=2, rule_timeout_sec=30,
                           run_deadline_sec=0.3, num_shards=2)
    t0 = time.monotonic()
    rep = runner.run(ds, rules)
    elapsed = time.monotonic() - t0
    log_report("卡死时限", ds, rules, rep, elapsed)

    assert elapsed < 1.0  # 墙钟耗时受控（含收尾），到点必须返回
    assert rep.status == RunStatus.PARTIAL
    assert len(rep.results) == 2
    assert all(r.status == CheckStatus.ERROR for r in rep.results)
    assert all("墙钟" in r.reason or "超时" in r.reason for r in rep.results)


def test_hung_rule_does_not_affect_other_results(monkeypatch, store, caplog):
    """一条卡死不影响同轮其他规则：快规则结论保留并留痕，卡住的记 error。"""
    caplog.set_level(logging.INFO)
    import dqgate.runner as runner_mod
    from dqgate.rules import execute_rule as real_execute

    def mixed_execute(rule, rows, shard):
        if rule.rule_id == "hung":
            time.sleep(30)
        return real_execute(rule, rows, shard)

    monkeypatch.setattr(runner_mod, "execute_rule", mixed_execute)
    ds = make_dataset([{"v": 1}, {"v": 2}])
    rules = [
        RuleDefinition("fast", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 10}),
        RuleDefinition("hung", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 10}),
    ]
    runner = QualityRunner(store=store, max_workers=2, rule_timeout_sec=30,
                           run_deadline_sec=0.5, num_shards=2)
    t0 = time.monotonic()
    rep = runner.run(ds, rules)
    elapsed = time.monotonic() - t0
    log_report("卡死隔离", ds, rules, rep, elapsed)

    assert elapsed < 1.5
    fast = [r for r in rep.results if r.rule_id == "fast"]
    hung = [r for r in rep.results if r.rule_id == "hung"]
    assert fast and all(r.status == CheckStatus.PASSED for r in fast)  # 其他规则照常出结论
    assert hung and all(r.status == CheckStatus.ERROR for r in hung)   # 卡住的：未得出结论
    assert rep.status == RunStatus.PARTIAL
    # 快规则结论已留痕；卡住的 error 也留痕（可重跑收敛），互不覆盖
    stored = store.query("mon-ds", dataset_digest=ds.digest)
    assert any(r["rule_id"] == "fast" and r["status"] == "passed" for r in stored)
    assert any(r["rule_id"] == "hung" and r["status"] == "error" for r in stored)
