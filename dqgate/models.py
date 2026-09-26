"""核心类型定义：规则、结果、运行报告。"""

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
    """单条规则的执行结论。"""

    PASSED = "passed"            # 校验通过
    FAILED = "failed"            # 校验不通过（数据问题）
    ERROR = "error"              # 规则执行失败（异常/超时）
    REJECTED = "rejected"        # 规则定义非法，被拒绝执行


class RunStatus(str, enum.Enum):
    """一次校验运行的终态。"""

    PASSED = "passed"      # 全部规则通过
    FAILED = "failed"      # 至少一条规则判定失败（无执行错误）
    PARTIAL = "partial"    # 存在执行失败/超时/被拒绝的规则，结论不完整


@dataclass(frozen=True)
class RuleDefinition:
    """一条带版本的数据质量规则定义。"""

    rule_id: str
    rule_type: RuleType
    version: int
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuleResult:
    """单条规则在单个分片上的执行结果（留痕，不可变）。"""

    rule_id: str
    rule_version: int
    shard: str
    status: CheckStatus
    reason: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunReport:
    """一次运行的整体报告：终态 + 各规则结果。"""

    run_id: str
    dataset_id: str
    dataset_digest: str
    status: RunStatus
    results: tuple[RuleResult, ...] = ()
