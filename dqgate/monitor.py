"""持续监控：按数据内容版本留痕、查询，并比对前后内容版本的结论差异。

违规身份（跨内容版本对齐的最小粒度）为 (rule_id, rule_version, shard)：
- NEW：新内容版本判定 failed、旧内容版本没有该违规；
- FIXED：旧内容版本有 failed、新内容版本已不再违规；
- UNRESOLVED：两个内容版本都违规（仍未解决）。

只对终态数据结论 failed 计为"违规"；error/rejected 表示"未得出结论"，
不计入修好/未解决，避免把没跑成功悄悄当作修好。
"""

from __future__ import annotations

from dqgate.models import (
    CheckStatus,
    ContentDiff,
    DiffKind,
    ViolationItem,
)
from dqgate.store import ResultStore


def list_content_versions(store: ResultStore, dataset_id: str) -> list[dict]:
    """列出数据集历史上跑过的所有数据内容版本（按首次出现顺序）。

    每项包含内容指纹、涉及的运行标识、结论条数与各状态计数，
    便于按数据集 + 内容版本分别追溯历史结论。
    """
    digests = store.list_digests(dataset_id)
    out: list[dict] = []
    for digest in digests:
        recs = store.query(dataset_id=dataset_id, dataset_digest=digest)
        run_ids: list[str] = []
        statuses: dict[str, int] = {}
        for rec in recs:
            if rec["run_id"] not in run_ids:
                run_ids.append(rec["run_id"])
            statuses[rec["status"]] = statuses.get(rec["status"], 0) + 1
        out.append({
            "dataset_id": dataset_id,
            "dataset_digest": digest,
            "run_ids": run_ids,
            "result_count": len(recs),
            "statuses": statuses,
        })
    return out


def _violations_by_key(records: list[dict]) -> dict[tuple, dict]:
    """从留痕记录中提取违规（failed）结论，按 (规则,版本,分片) 建索引。"""
    out: dict[tuple, dict] = {}
    for rec in records:
        if rec["status"] == CheckStatus.FAILED.value:
            out[(rec["rule_id"], rec["rule_version"], rec["shard"])] = rec
    return out


def _to_item(kind: DiffKind, rec: dict) -> ViolationItem:
    return ViolationItem(
        kind=kind,
        rule_id=rec["rule_id"],
        rule_version=rec["rule_version"],
        shard=rec["shard"],
        reason=rec.get("reason", ""),
        evidence=rec.get("evidence", {}),
    )


def diff_content(
    store: ResultStore,
    dataset_id: str,
    old_digest: str,
    new_digest: str,
    rule_version: int | None = None,
) -> ContentDiff:
    """对比同一数据集、同一规则版本下两个数据内容版本的违规差异。

    old_digest / new_digest 必须都已有留痕，否则抛 ValueError
    （不能拿不存在的历史版本做对比）。
    """
    if old_digest == new_digest:
        raise ValueError("对比的两个数据内容版本指纹相同，无需对比")

    old_records = store.query(
        dataset_id=dataset_id, dataset_digest=old_digest, rule_version=rule_version)
    new_records = store.query(
        dataset_id=dataset_id, dataset_digest=new_digest, rule_version=rule_version)
    if not old_records:
        raise ValueError(f"数据内容版本 {old_digest[:12]} 在数据集 {dataset_id} 下无历史结论")
    if not new_records:
        raise ValueError(f"数据内容版本 {new_digest[:12]} 在数据集 {dataset_id} 下无历史结论")

    old_bad = _violations_by_key(old_records)
    new_bad = _violations_by_key(new_records)

    new_keys = set(new_bad) - set(old_bad)
    fixed_keys = set(old_bad) - set(new_bad)
    unresolved_keys = set(old_bad) & set(new_bad)

    def sort_key(k: tuple) -> tuple:
        return k

    new_items = tuple(
        _to_item(DiffKind.NEW, new_bad[k]) for k in sorted(new_keys, key=sort_key))
    fixed_items = tuple(
        _to_item(DiffKind.FIXED, old_bad[k]) for k in sorted(fixed_keys, key=sort_key))
    unresolved_items = tuple(
        _to_item(DiffKind.UNRESOLVED, new_bad[k])
        for k in sorted(unresolved_keys, key=sort_key))

    return ContentDiff(
        dataset_id=dataset_id,
        rule_version=rule_version,
        old_digest=old_digest,
        new_digest=new_digest,
        new_items=new_items,
        fixed_items=fixed_items,
        unresolved_items=unresolved_items,
    )
