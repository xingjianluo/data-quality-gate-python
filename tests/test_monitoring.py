"""持续监控能力测试：内容版本化留痕与可复现、批次差异判定、整轮墙钟时限。

业务覆盖（日志会逐条打印用例输入与判定）：
1. 同一数据内容+同一规则版本 -> run_id 与结论跨时间/跨进程完全可复现；
   数据内容变化 -> 新旧内容版本结论各自留痕、不报错、不覆盖，可分别查出。
2. 两个数据内容版本之间的违规差异：新出现 / 已修好 / 未解决，含条数与
   规则、分片下钻。
3. 卡死规则受整轮墙钟时限约束：到点必须返回，卡死规则记 ERROR 不当通过，
   其他规则结论不受影响。
"""

from __future__ import annotations

import logging
import threading
import time

from dqgate.dataset import Dataset, compute_digest
from dqgate.models import CheckStatus, DiffKind, RuleDefinition, RuleResult, RuleType, RunStatus
from dqgate.monitor import diff_content, list_content_versions
from dqgate.runner import QualityRunner
from dqgate.store import ResultStore

log = logging.getLogger("dqgate.test")


def make_dataset(rows, dataset_id="t-ds") -> Dataset:
    return Dataset(dataset_id=dataset_id, digest=compute_digest(list(rows)), rows=tuple(rows))


def range_rule(version=1, hi=100, field="v"):
    return RuleDefinition("range", RuleType.VALUE_RANGE, version, {"field": field, "max": hi})


# ---------- 业务一：内容版本化留痕与可复现 ----------

def test_same_content_reproducible_run_id_across_restart(store, tmp_path, caplog):
    """同内容同规则：跨重跑、跨服务重启，run_id 与结论完全一致。"""
    caplog.set_level(logging.INFO)
    ds = make_dataset([{"v": 5}, {"v": 9}])
    rules = [range_rule(1)]
    runner = QualityRunner(store=store)

    rep1 = runner.run(ds, rules)
    log.info("[可复现] 第 1 次运行 run_id=%s digest=%s 终态=%s",
             rep1.run_id, ds.digest[:12], rep1.status.value)
    time.sleep(1.05)  # 明确跨越运行时刻，结论不得随时间漂移

    rep2 = runner.run(ds, rules)
    log.info("[可复现] 第 2 次运行 run_id=%s 终态=%s", rep2.run_id, rep2.status.value)
    assert rep2.run_id == rep1.run_id  # 确定性标识，与时间无关
    assert [ (r.shard, r.status.value, r.reason) for r in rep1.results ] == \
           [ (r.shard, r.status.value, r.reason) for r in rep2.results ]

    # 模拟服务重启：用同一个 JSONL 文件重新加载存储
    restarted_store = ResultStore(path=str(tmp_path / "results.jsonl"))
    rep3 = QualityRunner(store=restarted_store).run(ds, rules)
    log.info("[可复现] 重启后运行 run_id=%s 终态=%s", rep3.run_id, rep3.status.value)
    assert rep3.run_id == rep1.run_id
    assert rep3.status == rep1.status
    # 同一内容版本只留一组结论（幂等，不重复）
    assert len(store.query("t-ds", dataset_digest=ds.digest)) == 1


def test_changed_content_kept_separately_not_overwritten(store, caplog):
    """数据内容变化：同数据集同规则版本下，新老结论各自留痕、不冲突。"""
    caplog.set_level(logging.INFO)
    v1_rows = [{"v": 5}, {"v": 9}]
    v2_rows = [{"v": 5}, {"v": 150}]  # 内容变了：出现越界
    ds1 = make_dataset(v1_rows)
    ds2 = make_dataset(v2_rows)
    assert ds1.digest != ds2.digest
    rules = [range_rule(1)]
    runner = QualityRunner(store=store)

    rep_old = runner.run(ds1, rules)
    log.info("[版本留痕] 内容A digest=%s run_id=%s 终态=%s",
             ds1.digest[:12], rep_old.run_id, rep_old.status.value)
    rep_new = runner.run(ds2, rules)  # 不得报 DuplicateResultError
    log.info("[版本留痕] 内容B digest=%s run_id=%s 终态=%s",
             ds2.digest[:12], rep_new.run_id, rep_new.status.value)

    assert rep_old.run_id != rep_new.run_id  # 内容不同 -> 运行标识不同
    assert rep_old.status == RunStatus.PASSED
    assert rep_new.status == RunStatus.FAILED

    # 按数据集 + 内容版本分别可查，老结论未被顶掉
    old_recs = store.query("t-ds", dataset_digest=ds1.digest)
    new_recs = store.query("t-ds", dataset_digest=ds2.digest)
    assert {r["status"] for r in old_recs} == {"passed"}
    assert {r["status"] for r in new_recs} == {"failed"}

    versions = list_content_versions(store, "t-ds")
    digests = [v["dataset_digest"] for v in versions]
    log.info("[版本留痕] 历史内容版本=%s 计数=%s",
             [d[:12] for d in digests],
             [(v["statuses"]) for v in versions])
    assert digests == [ds1.digest, ds2.digest]  # 按首次出现顺序保留全部版本

    # 同一内容再跑一次仍然收敛到原 run_id，不产生第三个版本
    rep_repeat = runner.run(ds2, rules)
    assert rep_repeat.run_id == rep_new.run_id
    assert len(list_content_versions(store, "t-ds")) == 2


def test_different_rule_version_yet_content_history_coexists(store, caplog):
    """规则升级后，两个规则版本 x 多个内容版本的历史四象限都能分别查出。"""
    caplog.set_level(logging.INFO)
    ds1 = make_dataset([{"v": 50}])
    ds2 = make_dataset([{"v": 150}])
    runner = QualityRunner(store=store)
    r_v1 = range_rule(version=1, hi=200)
    r_v2 = range_rule(version=2, hi=100)

    runner.run(ds1, [r_v1]); runner.run(ds2, [r_v1])
    runner.run(ds1, [r_v2]); runner.run(ds2, [r_v2])

    # (内容A, 规则v1)=passed, (内容B, 规则v1)=passed(150<=200),
    # (内容A, 规则v2)=passed, (内容B, 规则v2)=failed(150>100)
    got = {
        (rec["dataset_digest"], rec["rule_version"]): rec["status"]
        for rec in store.query("t-ds", rule_id="range")
    }
    assert got[(ds1.digest, 1)] == "passed"
    assert got[(ds2.digest, 1)] == "passed"
    assert got[(ds1.digest, 2)] == "passed"
    assert got[(ds2.digest, 2)] == "failed"
    log.info("[四象限] 内容版本 x 规则版本历史=%s",
             {(d[:8], v): s for (d, v), s in got.items()})


# ---------- 业务二：批次差异判定 ----------

def _run_two_contents(store, caplog, rows_old, rows_new, num_shards=1, rules=None):
    caplog.set_level(logging.INFO)
    rules = rules or [range_rule(1)]
    ds_old = make_dataset(rows_old)
    ds_new = make_dataset(rows_new)
    runner = QualityRunner(store=store, num_shards=num_shards)
    runner.run(ds_old, rules)
    runner.run(ds_new, rules)
    return ds_old, ds_new, rules, runner


def test_diff_new_fixed_unresolved_with_counts_and_drilldown(store, caplog):
    """两内容版本差异：新出现/已修好/未解决，条数准确，可下钻规则与分片。"""
    rows_old = [{"id": 0, "v": 5}, {"id": 1, "v": 999}, {"id": 2, "v": 888}]
    rows_new = [{"id": 0, "v": 5}, {"id": 1, "v": 50}, {"id": 2, "v": 777},
                {"id": 3, "v": 666}]
    # 违规行：旧 = {1,2}；新 = {2,3} -> fixed={1}, unresolved={2}, new={3}
    ds_old, ds_new, _, _ = _run_two_contents(store, caplog, rows_old, rows_new, num_shards=4)

    diff = diff_content(store, "t-ds", ds_old.digest, ds_new.digest, rule_version=1)
    log.info("[批次差异] new=%d fixed=%d unresolved=%d",
             diff.new_count, diff.fixed_count, diff.unresolved_count)
    assert (diff.new_count, diff.fixed_count, diff.unresolved_count) == (1, 1, 1)

    by_kind = {
        DiffKind.NEW: diff.new_items,
        DiffKind.FIXED: diff.fixed_items,
        DiffKind.UNRESOLVED: diff.unresolved_items,
    }
    for kind, items in by_kind.items():
        for it in items:
            log.info("[批次差异] 类别=%s 规则=%s 版本=%s 分片=%s 依据=%s",
                     kind.value, it.rule_id, it.rule_version, it.shard, it.reason)
            assert it.rule_id == "range" and it.rule_version == 1
            assert it.shard.startswith("shard-")  # 可下钻到分片
            assert it.reason  # 每条差异都有判定依据

    # 行号 i 落在分片 i % 4
    assert {it.shard for it in diff.fixed_items} == {"shard-1"}
    assert {it.shard for it in diff.unresolved_items} == {"shard-2"}
    assert {it.shard for it in diff.new_items} == {"shard-3"}


def test_diff_all_fixed_and_rule_level_rollup(store, caplog):
    """全部修好时三类计数正确，并可按规则聚合查看下钻。"""
    caplog.set_level(logging.INFO)
    rules = [
        RuleDefinition("r1", RuleType.VALUE_RANGE, 3, {"field": "a", "max": 10}),
        RuleDefinition("r2", RuleType.VALUE_RANGE, 3, {"field": "b", "max": 10}),
    ]
    ds_old = make_dataset([{"a": 99, "b": 88}])
    ds_new = make_dataset([{"a": 1, "b": 2}])
    runner = QualityRunner(store=store)
    runner.run(ds_old, rules)
    runner.run(ds_new, rules)

    diff = diff_content(store, "t-ds", ds_old.digest, ds_new.digest)
    log.info("[批次差异-全修复] new=%d fixed=%d unresolved=%d fixed规则=%s",
             diff.new_count, diff.fixed_count, diff.unresolved_count,
             sorted(i.rule_id for i in diff.fixed_items))
    assert diff.new_count == 0 and diff.unresolved_count == 0 and diff.fixed_count == 2
    assert {i.rule_id for i in diff.fixed_items} == {"r1", "r2"}
    assert diff.rule_version is None  # 不传规则版本时覆盖该内容版本全部规则


def test_diff_unknown_digest_raises(store, caplog):
    """对比不存在的内容版本必须明确报错，而不是给出空差异。"""
    caplog.set_level(logging.INFO)
    ds = make_dataset([{"v": 1}])
    QualityRunner(store=store).run(ds, [range_rule(1)])
    try:
        diff_content(store, "t-ds", ds.digest, "f" * 64)
    except ValueError as e:
        log.info("[批次差异] 未知版本报错: %s", e)
    else:
        raise AssertionError("对不存在的内容版本做差异对比应当报错")


# ---------- 业务三：卡死规则下的整轮墙钟时限保证 ----------

def test_hard_deadline_returns_on_time_stuck_rule_error(store, caplog, monkeypatch):
    """卡死规则：整轮到点必须返回；卡死规则记 ERROR，其他规则结论保留。"""
    caplog.set_level(logging.INFO)
    ds = make_dataset([{"v": 1}])
    rules = [
        RuleDefinition("fast", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 10}),
        RuleDefinition("stuck", RuleType.VALUE_RANGE, 1, {"field": "v", "max": 10}),
    ]

    import dqgate.runner as runner_mod
    real_execute = runner_mod.execute_rule

    def stuck_execute(rule, shard_rows, shard):
        if rule.rule_id == "stuck":
            time.sleep(30)  # 模拟上游卡死
        return real_execute(rule, shard_rows, shard)

    monkeypatch.setattr(runner_mod, "execute_rule", stuck_execute)
    runner = QualityRunner(store=store, max_workers=2, rule_timeout_sec=30,
                           run_deadline_sec=0.4)
    t0 = time.monotonic()
    rep = runner.run(ds, rules)
    elapsed = time.monotonic() - t0
    log.info("[整轮时限] 墙钟耗时=%.3fs 终态=%s", elapsed, rep.status.value)
    for r in rep.results:
        log.info("[整轮时限] 规则=%s 分片=%s 结论=%s 依据=%s",
                 r.rule_id, r.shard, r.status.value, r.reason)

    assert elapsed < 1.0, f"整轮必须在时限内返回，实际 {elapsed:.2f}s"
    by_id = {r.rule_id: r for r in rep.results}
    assert by_id["stuck"].status == CheckStatus.ERROR
    assert "时限" in by_id["stuck"].reason and "未得出结论" in by_id["stuck"].reason
    assert by_id["fast"].status == CheckStatus.PASSED  # 其他规则结论不受影响
    assert rep.status == RunStatus.PARTIAL

    # 卡死规则同样留痕为 ERROR（未得出结论），可按内容版本查出
    recs = store.query("t-ds", dataset_digest=ds.digest)
    stuck_rec = next(r for r in recs if r["rule_id"] == "stuck")
    assert stuck_rec["status"] == "error"


def test_hard_deadline_with_queued_tasks_and_sharded_results(store, caplog, monkeypatch):
    """任务排队中时限到点也要返回；已完成分片的结论不丢。"""
    caplog.set_level(logging.INFO)
    rows = [{"id": i, "v": i} for i in range(8)]
    ds = make_dataset(rows)
    rules = [
        RuleDefinition("ok", RuleType.MISSING_FIELD, 1, {"field": "v"}),
        RuleDefinition("stuck", RuleType.MISSING_FIELD, 1, {"field": "v"}),
    ]

    import dqgate.runner as runner_mod
    real_execute = runner_mod.execute_rule
    gate = threading.Event()

    def stuck_execute(rule, shard_rows, shard):
        if rule.rule_id == "stuck":
            gate.wait(timeout=30)  # 卡住直到测试结束，霸占工作线程
        return real_execute(rule, shard_rows, shard)

    monkeypatch.setattr(runner_mod, "execute_rule", stuck_execute)
    runner = QualityRunner(store=store, max_workers=1, num_shards=2,
                           rule_timeout_sec=30, run_deadline_sec=0.4)
    t0 = time.monotonic()
    rep = runner.run(ds, rules)
    elapsed = time.monotonic() - t0
    gate.set()  # 释放卡死线程，避免长时间占用解释器退出
    log.info("[整轮时限-排队] 墙钟=%.3fs 结果条数=%d 终态=%s",
             elapsed, len(rep.results), rep.status.value)

    assert elapsed < 1.0
    # 2 规则 x 2 分片 = 4 条结论：能拿到的保留，卡住/排队的记 ERROR
    assert len(rep.results) == 4
    stuck = [r for r in rep.results if r.rule_id == "stuck"]
    ok = [r for r in rep.results if r.rule_id == "ok"]
    assert all(r.status == CheckStatus.ERROR for r in stuck)
    assert any(r.status == CheckStatus.PASSED for r in ok)  # 已完成结论不丢
    assert rep.status == RunStatus.PARTIAL


# ---------- 兼容：历史留痕文件（v1 无 run_id 字段） ----------

def test_legacy_jsonl_without_run_id_is_readable(tmp_path, caplog):
    """老版本写出的 JSONL（无 run_id）可直接读、可按内容版本查、可幂等补跑。"""
    caplog.set_level(logging.INFO)
    import json

    p = tmp_path / "legacy.jsonl"
    legacy_rec = {
        "dataset_id": "old-ds", "dataset_digest": "abc123",
        "rule_id": "range", "rule_version": 1, "shard": "shard-0",
        "status": "failed", "reason": "越界",
        "evidence": {"violation_count": 2},
    }
    p.write_text(json.dumps(legacy_rec, ensure_ascii=False) + "\n", encoding="utf-8")

    store = ResultStore(p)  # 不报错即可加载
    got = store.get("old-ds", "range", 1, "shard-0", dataset_digest="abc123")
    assert got["status"] == "failed" and got["run_id"] == "legacy"
    log.info("[兼容] 老记录读取成功 run_id=%s 状态=%s", got["run_id"], got["status"])

    # 同内容版本幂等补跑（新 run_id 元数据不导致冲突、不重复落盘）
    store.append("old-ds", "abc123",
                 [RuleResult(
                     "range", 1, "shard-0", CheckStatus.FAILED, "越界",
                     {"violation_count": 2})],
                 run_id="run-new")
    assert len(store.query("old-ds")) == 1
    log.info("[兼容] 老结论幂等补跑无冲突，历史结论条数=1")
