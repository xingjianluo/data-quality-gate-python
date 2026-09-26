"""规则执行与非法定义拒绝的单元测试。"""
import logging

import pytest

from dqgate.dataset import load_dataset
from dqgate.models import CheckStatus, RuleDefinition, RuleType
from dqgate.rules import RuleValidationError, execute_rule

logger = logging.getLogger("test.rules")


def test_all_rule_types_execute(base_rules):
    rows = load_dataset("orders")
    for rule in base_rules:
        res = execute_rule(rule, rows)
        logger.info("rule=%s version=%s status=%s reason=%s",
                    rule.rule_id, rule.version, res.status.value, res.reason)
        assert res.status in (CheckStatus.PASSED, CheckStatus.FAILED)


@pytest.mark.parametrize("params", [
    {},                                        # 缺 field
    {"field": "amount"},                       # 缺 min/max
    {"field": "amount", "min": 10, "max": 1},  # min > max
])
def test_invalid_value_range_rejected(params):
    rule = RuleDefinition("bad", RuleType.VALUE_RANGE, 1, params)
    with pytest.raises(RuleValidationError):
        execute_rule(rule, [])
    logger.info("rejected params=%s", params)


def test_reproducible_same_dataset_same_version(base_rules):
    rows = load_dataset("orders")
    first = [execute_rule(r, rows) for r in base_rules]
    second = [execute_rule(r, rows) for r in base_rules]
    assert [(r.status, r.reason) for r in first] == [(r.status, r.reason) for r in second]
