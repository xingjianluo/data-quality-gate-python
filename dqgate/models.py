"""核心数据模型：规则、结果、终态定义。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class RuleType(str, enum.Enum):
    """支持的校验规则类型。"""

    MISSING_FIELD = "missing_field"          # 字段缺失
    VALUE_RANGE = "value_range"              # 取值越界
    DUPLICATE_RECORD = "duplicate_record"    # 重复记录
    FRESHNESS = "freshness"                  # 数据新鲜度
    CROSS_FIELD = "cross_field"              # 跨字段一致性


class CheckStatus(str, enum.Enum):
    """单条规则的执行状态。"""

    PASSED = "passed"
    FAILED = "failed"          # 数据不满足规则
    ERROR = "error"            # 规则执行失败（异常/超时）
    REJECTED = "rejected"      # 规则定义非法，拒绝执行


class Verdict(str, enum.Enum):
    """一次校验运行的整体终态。"""

    PASSED = "passed"
    FAILED = "failed"
    PARTIAL = "partial"        # 部分规则失败/异常，未静默判通过


@dataclass(frozen=True)
class RuleDefinition:
    """规则定义（含版本）。同 rule_id 下 version 唯一标识一份定义。"""

    rule_id: str
    rule_type: RuleType
    version: int
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CheckResult:
    """单条规则对某数据集的校验结论（留痕单元，不可变）。"""

    rule_id: str
    rule_version: int
    dataset_id: str
    dataset_fingerprint: str
    status: CheckStatus
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunRecord:
    """一次校验运行的整体记录。"""

    run_id: str
    dataset_id: str
    dataset_fingerprint: str
    verdict: Verdict
    results: tuple[CheckResult, ...]
