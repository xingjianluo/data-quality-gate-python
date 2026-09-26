import logging

import pytest

from dqgate.models import RuleDefinition, RuleType


@pytest.fixture()
def base_rules() -> list[RuleDefinition]:
    """覆盖五类规则的基础规则集（version=1）。"""
    return [
        RuleDefinition("amount-range", RuleType.VALUE_RANGE, 1, {"field": "amount", "min": 0}),
        RuleDefinition("no-dup", RuleType.DUPLICATE_RECORD, 1, {"keys": ["order_id"]}),
        RuleDefinition("user-present", RuleType.MISSING_FIELD, 1, {"field": "user"}),
        RuleDefinition("fresh", RuleType.FRESHNESS, 1,
                       {"field": "updated_at", "max_age_seconds": 3600}),
        RuleDefinition("qty-consistent", RuleType.CROSS_FIELD, 1,
                       {"left": "quantity", "op": "ge", "right": "quantity"}),
    ]


def log_run_summary(rec, logger: logging.Logger) -> None:
    """单测日志：打印输入摘要、规则版本与判定依据。"""
    logger.info("run_id=%s dataset=%s fingerprint=%s verdict=%s",
                rec.run_id, rec.dataset_id, rec.dataset_fingerprint[:12], rec.verdict.value)
    for r in rec.results:
        logger.info("  rule=%s version=%s status=%s reason=%s evidence=%s",
                    r.rule_id, r.rule_version, r.status.value, r.reason, r.evidence)
