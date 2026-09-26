"""规则注册与执行。

约定：
- validate_rule 对非法定义抛 RuleValidationError（调用方映射为 REJECTED）。
- execute_rule 只返回 PASSED/FAILED；执行异常由调用方捕获并映射为 ERROR。
- 所有规则对同一数据集 + 同一规则版本的结论必须可复现（不使用 wall-clock，
  新鲜度规则的参考时间来自参数或数据本身）。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from .models import CheckResult, CheckStatus, RuleDefinition, RuleType


class RuleValidationError(ValueError):
    """规则定义非法。"""


def _parse_ts(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"not a timestamp string: {value!r}")
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


# ---------------------------------------------------------------- 各规则执行器
def _check_missing_field(rule: RuleDefinition, rows: list[dict[str, Any]]) -> CheckResult:
    field = rule.params["field"]
    missing = [i for i, r in enumerate(rows) if field not in r or r[field] is None]
    ok = not missing
    return CheckResult(
        rule_id=rule.rule_id, rule_version=rule.version,
        dataset_id="", dataset_fingerprint="",
        status=CheckStatus.PASSED if ok else CheckStatus.FAILED,
        reason="all rows have field" if ok else f"{len(missing)} row(s) missing field '{field}'",
        evidence={"field": field, "missing_row_indexes": missing[:20]},
    )


def _check_value_range(rule: RuleDefinition, rows: list[dict[str, Any]]) -> CheckResult:
    field = rule.params["field"]
    lo, hi = rule.params.get("min"), rule.params.get("max")
    bad = []
    for i, r in enumerate(rows):
        v = r.get(field)
        if v is None or not isinstance(v, (int, float)):
            bad.append({"row": i, "value": v, "cause": "non-numeric"})
            continue
        if (lo is not None and v < lo) or (hi is not None and v > hi):
            bad.append({"row": i, "value": v, "cause": "out-of-range"})
    ok = not bad
    return CheckResult(
        rule_id=rule.rule_id, rule_version=rule.version,
        dataset_id="", dataset_fingerprint="",
        status=CheckStatus.PASSED if ok else CheckStatus.FAILED,
        reason="all values in range" if ok else f"{len(bad)} row(s) out of range [{lo}, {hi}]",
        evidence={"field": field, "min": lo, "max": hi, "violations": bad[:20]},
    )


def _check_duplicate(rule: RuleDefinition, rows: list[dict[str, Any]]) -> CheckResult:
    keys = rule.params["keys"]
    seen: dict[tuple, int] = {}
    dups: list[dict[str, Any]] = []
    for i, r in enumerate(rows):
        k = tuple(r.get(f) for f in keys)
        if k in seen:
            dups.append({"row": i, "first_row": seen[k], "key": list(k)})
        else:
            seen[k] = i
    ok = not dups
    return CheckResult(
        rule_id=rule.rule_id, rule_version=rule.version,
        dataset_id="", dataset_fingerprint="",
        status=CheckStatus.PASSED if ok else CheckStatus.FAILED,
        reason="no duplicates" if ok else f"{len(dups)} duplicate row(s) on keys {keys}",
        evidence={"keys": keys, "duplicates": dups[:20]},
    )


def _check_freshness(rule: RuleDefinition, rows: list[dict[str, Any]]) -> CheckResult:
    field = rule.params["field"]
    max_age = rule.params["max_age_seconds"]
    ref_raw = rule.params.get("reference_time")
    if ref_raw is not None:
        ref = _parse_ts(ref_raw)
    else:
        # 参考时间取数据内最大时间戳，保证同数据同版本结论可复现
        ref = max((_parse_ts(r[field]) for r in rows if r.get(field)), default=None)
        if ref is None:
            raise ValueError(f"no parseable timestamps in field '{field}'")
    stale = []
    for i, r in enumerate(rows):
        if not r.get(field):
            continue
        age = (ref - _parse_ts(r[field])).total_seconds()
        if age > max_age:
            stale.append({"row": i, "age_seconds": age})
    ok = not stale
    return CheckResult(
        rule_id=rule.rule_id, rule_version=rule.version,
        dataset_id="", dataset_fingerprint="",
        status=CheckStatus.PASSED if ok else CheckStatus.FAILED,
        reason="all rows fresh" if ok else f"{len(stale)} row(s) older than {max_age}s",
        evidence={"field": field, "max_age_seconds": max_age,
                  "reference_time": ref.isoformat(), "stale": stale[:20]},
    )


_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "gt": lambda a, b: a > b,
    "ge": lambda a, b: a >= b,
    "lt": lambda a, b: a < b,
    "le": lambda a, b: a <= b,
}


def _check_cross_field(rule: RuleDefinition, rows: list[dict[str, Any]]) -> CheckResult:
    left, op, right = rule.params["left"], rule.params["op"], rule.params["right"]
    cmp_fn = _OPS[op]
    bad = []
    for i, r in enumerate(rows):
        lv, rv = r.get(left), r.get(right)
        if lv is None or rv is None:
            bad.append({"row": i, "left": lv, "right": rv, "cause": "missing operand"})
            continue
        try:
            ok_row = cmp_fn(lv, rv)
        except TypeError:
            bad.append({"row": i, "left": lv, "right": rv, "cause": "incomparable"})
            continue
        if not ok_row:
            bad.append({"row": i, "left": lv, "right": rv, "cause": f"not ({left} {op} {right})"})
    ok = not bad
    return CheckResult(
        rule_id=rule.rule_id, rule_version=rule.version,
        dataset_id="", dataset_fingerprint="",
        status=CheckStatus.PASSED if ok else CheckStatus.FAILED,
        reason="cross-field constraint holds" if ok else f"{len(bad)} row(s) violate {left} {op} {right}",
        evidence={"left": left, "op": op, "right": right, "violations": bad[:20]},
    )


_EXECUTORS: dict[RuleType, Callable[[RuleDefinition, list[dict[str, Any]]], CheckResult]] = {
    RuleType.MISSING_FIELD: _check_missing_field,
    RuleType.VALUE_RANGE: _check_value_range,
    RuleType.DUPLICATE_RECORD: _check_duplicate,
    RuleType.FRESHNESS: _check_freshness,
    RuleType.CROSS_FIELD: _check_cross_field,
}


# ---------------------------------------------------------------- 定义校验
def validate_rule(rule: RuleDefinition) -> None:
    """校验规则定义合法性，非法时抛 RuleValidationError。"""
    if not rule.rule_id:
        raise RuleValidationError("rule_id must be non-empty")
    if rule.version < 1:
        raise RuleValidationError("version must be >= 1")
    p = rule.params
    t = rule.rule_type
    if t is RuleType.MISSING_FIELD:
        if not isinstance(p.get("field"), str) or not p.get("field"):
            raise RuleValidationError("missing_field requires non-empty 'field'")
    elif t is RuleType.VALUE_RANGE:
        if not isinstance(p.get("field"), str) or not p.get("field"):
            raise RuleValidationError("value_range requires non-empty 'field'")
        lo, hi = p.get("min"), p.get("max")
        if lo is None and hi is None:
            raise RuleValidationError("value_range requires at least one of 'min'/'max'")
        for name, v in (("min", lo), ("max", hi)):
            if v is not None and not isinstance(v, (int, float)):
                raise RuleValidationError(f"value_range '{name}' must be numeric")
        if lo is not None and hi is not None and lo > hi:
            raise RuleValidationError("value_range min must be <= max")
    elif t is RuleType.DUPLICATE_RECORD:
        keys = p.get("keys")
        if not isinstance(keys, list) or not keys or not all(isinstance(k, str) and k for k in keys):
            raise RuleValidationError("duplicate_record requires non-empty list 'keys'")
    elif t is RuleType.FRESHNESS:
        if not isinstance(p.get("field"), str) or not p.get("field"):
            raise RuleValidationError("freshness requires non-empty 'field'")
        if not isinstance(p.get("max_age_seconds"), (int, float)) or p["max_age_seconds"] < 0:
            raise RuleValidationError("freshness requires non-negative 'max_age_seconds'")
        if p.get("reference_time") is not None:
            _parse_ts(p["reference_time"])
    elif t is RuleType.CROSS_FIELD:
        for name in ("left", "right"):
            if not isinstance(p.get(name), str) or not p.get(name):
                raise RuleValidationError(f"cross_field requires non-empty '{name}'")
        if p.get("op") not in _OPS:
            raise RuleValidationError(f"cross_field op must be one of {sorted(_OPS)}")
    else:  # pragma: no cover - 防御未知类型
        raise RuleValidationError(f"unknown rule type: {t}")


def execute_rule(rule: RuleDefinition, rows: list[dict[str, Any]]) -> CheckResult:
    """执行单条规则。定义非法抛 RuleValidationError，执行异常向上抛。"""
    validate_rule(rule)
    return _EXECUTORS[rule.rule_type](rule, rows)
