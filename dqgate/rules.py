"""规则定义校验与各类规则的执行实现。

约定：
- 规则定义非法 -> validate_rule 抛出 RuleRejectedError（对应结论 REJECTED）。
- 规则执行期间抛出的其他异常 -> 由调用方转换为 ERROR（执行失败）。
- 只有真正执行完成才返回 PASSED / FAILED，绝不静默跳过或默认通过。
"""

from __future__ import annotations

import time
from typing import Any

from dqgate.models import CheckStatus, RuleDefinition, RuleResult, RuleType

_MAX_EVIDENCE_SAMPLES = 5


class RuleRejectedError(ValueError):
    """规则定义非法时抛出，携带可区分的拒绝原因。"""


_OPS = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "lt": lambda a, b: a < b,
    "le": lambda a, b: a <= b,
    "gt": lambda a, b: a > b,
    "ge": lambda a, b: a >= b,
}


def validate_rule(rule: RuleDefinition) -> None:
    """校验规则定义合法性；非法时抛出 RuleRejectedError。"""
    if not rule.rule_id or not str(rule.rule_id).strip():
        raise RuleRejectedError("rule_id 不能为空")
    if not isinstance(rule.version, int) or rule.version < 1:
        raise RuleRejectedError(f"规则版本必须是 >=1 的整数，实际为 {rule.version!r}")
    if not isinstance(rule.rule_type, RuleType):
        raise RuleRejectedError(f"未知规则类型: {rule.rule_type!r}")
    p = rule.params
    t = rule.rule_type
    if t is RuleType.MISSING_FIELD:
        _require_str(p, "field")
    elif t is RuleType.VALUE_RANGE:
        _require_str(p, "field")
        if "min" not in p and "max" not in p:
            raise RuleRejectedError("value_range 规则至少需要 min 或 max 之一")
        for k in ("min", "max"):
            if k in p and not isinstance(p[k], (int, float)):
                raise RuleRejectedError(f"value_range.{k} 必须是数值，实际为 {p[k]!r}")
    elif t is RuleType.DUPLICATE_RECORD:
        keys = p.get("keys")
        if not isinstance(keys, list) or not keys or not all(isinstance(k, str) and k for k in keys):
            raise RuleRejectedError("duplicate_record 规则需要非空字符串列表参数 keys")
    elif t is RuleType.FRESHNESS:
        _require_str(p, "field")
        age = p.get("max_age_seconds")
        if not isinstance(age, (int, float)) or age < 0:
            raise RuleRejectedError(f"freshness.max_age_seconds 必须是非负数值，实际为 {age!r}")
        if "now" in p and not isinstance(p["now"], (int, float)):
            raise RuleRejectedError("freshness.now 必须是数值时间戳")
    elif t is RuleType.CROSS_FIELD:
        _require_str(p, "left")
        _require_str(p, "right")
        op = p.get("op")
        if op not in _OPS:
            raise RuleRejectedError(f"cross_field.op 必须是 {sorted(_OPS)} 之一，实际为 {op!r}")


def _require_str(p: dict, key: str) -> None:
    if not isinstance(p.get(key), str) or not p[key]:
        raise RuleRejectedError(f"缺少合法参数 {key!r}（非空字符串）")


def execute_rule(rule: RuleDefinition, rows: tuple[dict, ...], shard: str) -> RuleResult:
    """执行单条规则，返回 PASSED/FAILED 结果；执行异常向上抛出。"""
    validate_rule(rule)
    handler = _HANDLERS[rule.rule_type]
    status, reason, evidence = handler(rule.params, rows)
    return RuleResult(
        rule_id=rule.rule_id,
        rule_version=rule.version,
        shard=shard,
        status=status,
        reason=reason,
        evidence=evidence,
    )


def _missing_field(p: dict, rows: tuple[dict, ...]):
    field = p["field"]
    bad = [i for i, r in enumerate(rows) if field not in r or r[field] is None or r[field] == ""]
    evidence = {"field": field, "row_count": len(rows), "violation_count": len(bad),
                "sample_rows": bad[:_MAX_EVIDENCE_SAMPLES]}
    if bad:
        return CheckStatus.FAILED, f"字段 {field!r} 在 {len(bad)}/{len(rows)} 行缺失或为空", evidence
    return CheckStatus.PASSED, f"字段 {field!r} 在全部 {len(rows)} 行均存在且非空", evidence


def _value_range(p: dict, rows: tuple[dict, ...]):
    field, lo, hi = p["field"], p.get("min"), p.get("max")
    bad = []
    for i, r in enumerate(rows):
        v = r.get(field)
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            bad.append({"row": i, "value": v, "why": "非数值或缺失"})
        elif lo is not None and v < lo:
            bad.append({"row": i, "value": v, "why": f"小于下界 {lo}"})
        elif hi is not None and v > hi:
            bad.append({"row": i, "value": v, "why": f"大于上界 {hi}"})
    evidence = {"field": field, "min": lo, "max": hi, "row_count": len(rows),
                "violation_count": len(bad), "samples": bad[:_MAX_EVIDENCE_SAMPLES]}
    if bad:
        return CheckStatus.FAILED, f"字段 {field!r} 有 {len(bad)}/{len(rows)} 行越界或非数值", evidence
    return CheckStatus.PASSED, f"字段 {field!r} 全部 {len(rows)} 行在取值范围内", evidence


def _duplicate_record(p: dict, rows: tuple[dict, ...]):
    keys = p["keys"]
    seen: dict[tuple, int] = {}
    dup_samples = []
    dup_count = 0
    for i, r in enumerate(rows):
        sig = tuple(r.get(k) for k in keys)
        if sig in seen:
            dup_count += 1
            if len(dup_samples) < _MAX_EVIDENCE_SAMPLES:
                dup_samples.append({"row": i, "first_row": seen[sig], "key": dict(zip(keys, sig))})
        else:
            seen[sig] = i
    evidence = {"keys": keys, "row_count": len(rows), "duplicate_count": dup_count,
                "samples": dup_samples}
    if dup_count:
        return CheckStatus.FAILED, f"按键 {keys} 发现 {dup_count} 条重复记录", evidence
    return CheckStatus.PASSED, f"按键 {keys} 无重复记录（共 {len(rows)} 行）", evidence


def _freshness(p: dict, rows: tuple[dict, ...]):
    field, max_age = p["field"], p["max_age_seconds"]
    now = p.get("now", time.time())  # 可用参数固定参考时间，保证可复现
    newest = None
    for r in rows:
        v = r.get(field)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            newest = v if newest is None else max(newest, v)
    evidence = {"field": field, "max_age_seconds": max_age, "now": now,
                "newest_ts": newest, "row_count": len(rows)}
    if newest is None:
        return CheckStatus.FAILED, f"字段 {field!r} 无任何有效时间戳", evidence
    age = now - newest
    evidence["age_seconds"] = age
    if age > max_age:
        return CheckStatus.FAILED, f"最新数据距参考时间 {age:.1f}s，超过阈值 {max_age}s", evidence
    return CheckStatus.PASSED, f"最新数据距参考时间 {age:.1f}s，未超阈值 {max_age}s", evidence


def _cross_field(p: dict, rows: tuple[dict, ...]):
    left, right, op = p["left"], p["right"], p["op"]
    fn = _OPS[op]
    bad = []
    for i, r in enumerate(rows):
        a, b = r.get(left), r.get(right)
        if a is None or b is None:
            bad.append({"row": i, "left": a, "right": b, "why": "字段缺失"})
        else:
            try:
                ok = fn(a, b)
            except TypeError:
                bad.append({"row": i, "left": a, "right": b, "why": "类型不可比较"})
                continue
            if not ok:
                bad.append({"row": i, "left": a, "right": b, "why": f"不满足 {left} {op} {right}"})
    evidence = {"left": left, "right": right, "op": op, "row_count": len(rows),
                "violation_count": len(bad), "samples": bad[:_MAX_EVIDENCE_SAMPLES]}
    if bad:
        return CheckStatus.FAILED, f"{len(bad)}/{len(rows)} 行不满足 {left} {op} {right}", evidence
    return CheckStatus.PASSED, f"全部 {len(rows)} 行满足 {left} {op} {right}", evidence


_HANDLERS = {
    RuleType.MISSING_FIELD: _missing_field,
    RuleType.VALUE_RANGE: _value_range,
    RuleType.DUPLICATE_RECORD: _duplicate_record,
    RuleType.FRESHNESS: _freshness,
    RuleType.CROSS_FIELD: _cross_field,
}
