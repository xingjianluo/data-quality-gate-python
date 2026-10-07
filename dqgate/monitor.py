"""持续监控：跨数据内容版本的结论差异对比。

在同一数据集、同一套规则版本下，对比前后两个内容版本的结论：
- 新出现的违规（new_violations）：旧版本 passed、新版本 failed
- 已修好的违规（fixed_violations）：旧版本 failed、新版本 passed
- 仍未解决的违规（unresolved_violations）：前后均 failed
- 未得出结论（inconclusive）：任一侧为 error/rejected 或两侧状态组合
  不属于上述三类的键（绝不悄悄算作修好或仍在）

每条差异定位到 (rule_id, rule_version, shard)，并可按规则/分片汇总条数。
"""

from __future__ import annotations

from dataclasses import dataclass

from dqgate.models import CheckStatus, RuleResult

_TERMINAL_OK = CheckStatus.PASSED
_TERMINAL_BAD = CheckStatus.FAILED


@dataclass(frozen=True)
class ViolationChange:
    """一条违规差异：定位到 (规则, 版本, 分片)。"""

    rule_id: str
    rule_version: int
    shard: str
    old_status: str | None = None
    new_status: str | None = None


@dataclass(frozen=True)
class BatchDiff:
    """两个内容版本之间的结论差异。"""

    dataset_id: str
    old_digest: str
    new_digest: str
    new_violations: tuple[ViolationChange, ...] = ()
    fixed_violations: tuple[ViolationChange, ...] = ()
    unresolved_violations: tuple[ViolationChange, ...] = ()
    inconclusive: tuple[ViolationChange, ...] = ()

    @property
    def counts(self) -> dict[str, int]:
        """各分类条数（监控盘一眼看全；inconclusive 单列，不混入其他类）。"""
        return {
            "new_violations": len(self.new_violations),
            "fixed_violations": len(self.fixed_violations),
            "unresolved_violations": len(self.unresolved_violations),
            "inconclusive": len(self.inconclusive),
        }

    def counts_by_rule(self) -> dict[str, dict[str, int]]:
        """按规则汇总四类条数，下钻用：{rule_id: {分类: 条数}}。"""
        return _aggregate(self, lambda c: c.rule_id)

    def counts_by_shard(self) -> dict[str, dict[str, int]]:
        """按分片汇总四类条数，下钻用：{shard: {分类: 条数}}。"""
        return _aggregate(self, lambda c: c.shard)


_CATEGORIES = (
    "new_violations",
    "fixed_violations",
    "unresolved_violations",
    "inconclusive",
)


def _aggregate(diff: "BatchDiff", key_fn) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for category in _CATEGORIES:
        for change in getattr(diff, category):
            k = key_fn(change)
            slot = out.setdefault(k, {c: 0 for c in _CATEGORIES})
            slot[category] += 1
    return dict(sorted(out.items()))


def _key(r: RuleResult) -> tuple[str, int, str]:
    return (r.rule_id, r.rule_version, r.shard)


def _index(results) -> dict[tuple[str, int, str], RuleResult]:
    """按 (规则, 版本, 分片) 建索引；同键取后者（仅非终态重跑会出现同键多条）。"""
    idx: dict[tuple[str, int, str], RuleResult] = {}
    for r in results:
        idx[_key(r)] = r
    return idx


def diff_results(
    dataset_id: str,
    old_digest: str,
    new_digest: str,
    old_results: list[RuleResult] | tuple[RuleResult, ...],
    new_results: list[RuleResult] | tuple[RuleResult, ...],
) -> BatchDiff:
    """对比同一数据集、同一规则版本下两个内容版本的结论。

    两侧键的并集逐个分类，保证新内容版本新增/删除分片时也能判定。
    """
    old_idx = _index(old_results)
    new_idx = _index(new_results)

    buckets: dict[str, list[ViolationChange]] = {
        "new_violations": [],
        "fixed_violations": [],
        "unresolved_violations": [],
        "inconclusive": [],
    }

    for key in sorted(set(old_idx) | set(new_idx)):
        o = old_idx.get(key)
        n = new_idx.get(key)
        os_ = o.status if o is not None else None
        ns = n.status if n is not None else None
        change = ViolationChange(
            rule_id=key[0], rule_version=key[1], shard=key[2],
            old_status=os_.value if os_ else None,
            new_status=ns.value if ns else None,
        )
        if os_ == _TERMINAL_OK and ns == _TERMINAL_BAD:
            buckets["new_violations"].append(change)
        elif os_ == _TERMINAL_BAD and ns == _TERMINAL_OK:
            buckets["fixed_violations"].append(change)
        elif os_ == _TERMINAL_BAD and ns == _TERMINAL_BAD:
            buckets["unresolved_violations"].append(change)
        else:
            # error/rejected/缺失/同为通过 等其余组合，一律归入未得出结论外的
            # “不可判定差异”；两侧同为 passed 的无差异键不产生告警但计入此类会
            # 产生噪声，因此同为 passed 时跳过。
            if os_ == _TERMINAL_OK and ns == _TERMINAL_OK:
                continue
            buckets["inconclusive"].append(change)

    return BatchDiff(
        dataset_id=dataset_id,
        old_digest=old_digest,
        new_digest=new_digest,
        new_violations=tuple(buckets["new_violations"]),
        fixed_violations=tuple(buckets["fixed_violations"]),
        unresolved_violations=tuple(buckets["unresolved_violations"]),
        inconclusive=tuple(buckets["inconclusive"]),
    )


def diff_stored(store, dataset_id: str, old_digest: str, new_digest: str) -> BatchDiff:
    """从留痕存储取两个内容版本的最新结论做差异对比。

    只取两侧都出现过的 (规则, 版本) 集合，避免某侧只跑过部分规则时
    把“没跑”误判为修复；缺失侧的键仍会在 diff_results 内归入 inconclusive。
    """
    old_records = store.query(dataset_id=dataset_id, dataset_digest=old_digest)
    new_records = store.query(dataset_id=dataset_id, dataset_digest=new_digest)
    old_results = [_record_to_result(r) for r in old_records]
    new_results = [_record_to_result(r) for r in new_records]
    return diff_results(dataset_id, old_digest, new_digest, old_results, new_results)


def _record_to_result(rec: dict) -> RuleResult:
    return RuleResult(
        rule_id=rec["rule_id"],
        rule_version=rec["rule_version"],
        shard=rec["shard"],
        status=CheckStatus(rec["status"]),
        reason=rec.get("reason", ""),
        evidence=rec.get("evidence", {}),
    )
