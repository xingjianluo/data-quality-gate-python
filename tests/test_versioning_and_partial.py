"""规则版本切换、结论留痕与部分失败重跑。"""
import logging

import pytest

from dqgate.models import CheckStatus, RuleDefinition, RuleType, Verdict
from dqgate.runner import CheckRunner
from dqgate.store import ResultStore
from tests.conftest import log_run_summary

logger = logging.getLogger("test.versioning")


def _range_rule(version: int, min_v: float) -> RuleDefinition:
    return RuleDefinition("amount-range", RuleType.VALUE_RANGE, version,
                          {"field": "amount", "min": min_v})


def test_version_switch_preserves_history(tmp_path):
    """同一数据集在 v1/v2 下各跑一次：历史不被覆盖，可按版本区分查询。"""
    store = ResultStore(str(tmp_path / "results.jsonl"))
    runner = CheckRunner()

    rec_v1 = runner.run("orders", [_range_rule(1, 0)])
    store.append_run(rec_v1)
    log_run_summary(rec_v1, logger)

    rec_v2 = runner.run("orders", [_range_rule(2, -10)])  # 规则更新：放宽下界
    store.append_run(rec_v2)
    log_run_summary(rec_v2, logger)

    v1 = store.list_results("orders", rule_id="amount-range", rule_version=1)
    v2 = store.list_results("orders", rule_id="amount-range", rule_version=2)
    assert len(v1) == 1 and v1[0].status is CheckStatus.FAILED   # v1 结论未被 v2 篡改
    assert len(v2) == 1 and v2[0].status is CheckStatus.PASSED
    assert len(store.list_results("orders", rule_id="amount-range")) == 2
    # 同版本重跑结论可复现
    rec_v1_again = runner.run("orders", [_range_rule(1, 0)])
    assert rec_v1_again.results[0].status is CheckStatus.FAILED
    assert rec_v1_again.results[0].reason == v1[0].reason


def test_partial_failure_and_rerun_only_failed(tmp_path, base_rules):
    """部分规则异常 -> PARTIAL；只重跑失败部分，已成功部分不重复执行。"""
    store = ResultStore(str(tmp_path / "results.jsonl"))
    runner = CheckRunner()

    bad = RuleDefinition("bad-rule", RuleType.VALUE_RANGE, 1, {"field": "amount"})  # 非法定义
    rec1 = runner.run("orders", base_rules + [bad])
    store.append_run(rec1)
    log_run_summary(rec1, logger)
    assert rec1.verdict is Verdict.PARTIAL
    by_id = {r.rule_id: r for r in rec1.results}
    assert by_id["bad-rule"].status is CheckStatus.REJECTED
    assert "invalid rule definition" in by_id["bad-rule"].reason  # 拒绝原因可区分

    # 修复规则（升版本），只重跑 bad-rule，其余复用 rec1 结论
    fixed = RuleDefinition("bad-rule", RuleType.VALUE_RANGE, 2,
                           {"field": "amount", "min": -10})
    rules_v2 = [fixed if r.rule_id == "bad-rule" else r for r in base_rules + [bad]]
    rec2 = runner.run("orders", rules_v2, only_rule_ids={"bad-rule"}, prior=rec1)
    store.append_run(rec2)
    log_run_summary(rec2, logger)

    by_id2 = {r.rule_id: r for r in rec2.results}
    assert by_id2["bad-rule"].status is CheckStatus.PASSED
    assert by_id2["bad-rule"].rule_version == 2
    # 复用的结论与首次运行逐字节一致（未重复执行、未被污染）
    assert by_id2["qty-consistent"] == by_id["qty-consistent"]
    # 仍有真实 FAILED -> 整体 FAILED，绝不静默通过
    assert rec2.verdict is Verdict.FAILED

    # 重跑不覆盖历史：bad-rule 的 v1 拒绝记录仍可查
    rejected = store.list_results("orders", rule_id="bad-rule", rule_version=1)
    assert len(rejected) == 1 and rejected[0].status is CheckStatus.REJECTED


def test_partial_rerun_requires_prior(tmp_path, base_rules):
    runner = CheckRunner()
    with pytest.raises(ValueError, match="prior"):
        runner.run("orders", base_rules, only_rule_ids={"no-dup"}, prior=None)
