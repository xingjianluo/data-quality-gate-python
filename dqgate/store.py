"""结论留痕存储：追加写、按数据内容版本与规则版本查询、不覆盖既有结论。

存储格式为 JSONL，每行一条 RuleResult 记录，附带数据集标识、数据内容指纹
与运行标识。键为 (dataset_id, dataset_digest, rule_id, rule_version, shard)：
- 数据内容指纹（digest）入键：同一数据集、同一规则版本，数据内容变化后会形成
  新的一组键，新老内容版本的结论各自留痕、互不覆盖，也不会报冲突；
- 键不存在 -> 追加写入；
- 键已存在且内容完全一致 -> 幂等 no-op（同一数据内容同一规则版本的结论可复现，
  重复记录相同结论不构成篡改）；
- 键已存在、内容不一致、且既有结论为终态结论（passed/failed）
  -> 拒绝（DuplicateResultError），既不篡改也不覆盖；
- 键已存在但既有记录为非终态（error/rejected，表示"未得出结论"）
  -> 允许追加新记录并取代索引（文件仍为追加写，旧记录留痕可查，
  重跑恢复后结论可收敛，且历史失败痕迹不丢失）。

兼容：v1 版本写出的记录没有 run_id 字段，读取时归一化为 "legacy"；
其 dataset_digest 字段在 v1 中已存在，内容版本化对历史留痕直接生效。
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

from dqgate.models import RuleResult


class DuplicateResultError(RuntimeError):
    """同一 (数据集, 内容版本, 规则, 版本, 分片) 已存在不一致的终态结论，拒绝覆盖。"""


_NON_TERMINAL = {"error", "rejected"}  # 非终态：未得出结论，允许重跑后取代
_LEGACY_RUN_ID = "legacy"              # 历史记录（无 run_id 字段）的归一化标识
# 结论本体字段：run_id 仅为留痕元数据，不参与"结论是否一致"的判定，
# 否则历史记录（run_id=legacy）与新记录会被误判为冲突
_BODY_FIELDS = ("status", "reason", "evidence")


def _same_body(a: dict | None, b: dict | None) -> bool:
    """两条记录的结论本体（状态/依据/证据）是否一致（忽略 run_id 元数据）。"""
    if a is None or b is None:
        return False
    return all(a.get(f) == b.get(f) for f in _BODY_FIELDS)


class ResultStore:
    """基于 JSONL 的追加式结果存储（线程安全）。"""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._records: dict[tuple, dict] = {}
        # 内容版本 / 运行的出现顺序（用于历史列表稳定排序）
        self._digest_order: dict[str, list[str]] = {}
        self._run_order: list[str] = []
        if self._path.exists():
            for rec in self._read_all():
                self._index(rec)

    @staticmethod
    def _key_of(rec: dict) -> tuple:
        return (
            rec["dataset_id"],
            rec["dataset_digest"],
            rec["rule_id"],
            rec["rule_version"],
            rec["shard"],
        )

    def _index(self, rec: dict) -> None:
        """将一条记录纳入内存索引（读取历史或新写入后调用）。"""
        rec.setdefault("run_id", _LEGACY_RUN_ID)
        self._records[self._key_of(rec)] = rec
        digests = self._digest_order.setdefault(rec["dataset_id"], [])
        if rec["dataset_digest"] not in digests:
            digests.append(rec["dataset_digest"])
        run_id = rec["run_id"]
        if run_id not in self._run_order:
            self._run_order.append(run_id)

    def _read_all(self) -> list[dict]:
        out = []
        with self._path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out

    def append(
        self,
        dataset_id: str,
        dataset_digest: str,
        results: list[RuleResult],
        run_id: str = _LEGACY_RUN_ID,
    ) -> None:
        """追加一批结果；相同键相同内容为幂等 no-op，内容冲突则整批拒绝。"""
        records = [
            {
                "dataset_id": dataset_id,
                "dataset_digest": dataset_digest,
                "run_id": run_id,
                "rule_id": r.rule_id,
                "rule_version": r.rule_version,
                "shard": r.shard,
                "status": r.status.value,
                "reason": r.reason,
                "evidence": r.evidence,
            }
            for r in results
        ]
        with self._lock:
            seen: dict[tuple, dict] = {}
            for rec in records:
                key = self._key_of(rec)
                existing = seen.get(key, self._records.get(key))
                if (
                    existing is not None
                    and not _same_body(existing, rec)
                    and existing["status"] not in _NON_TERMINAL
                ):
                    raise DuplicateResultError(f"结论已存在且内容不一致，拒绝覆盖: {key}")
                seen[key] = rec
            to_write = [rec for key, rec in seen.items()
                        if not _same_body(self._records.get(key), rec)]
            if to_write:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                with self._path.open("a", encoding="utf-8") as f:
                    for rec in to_write:
                        f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")
                for rec in to_write:
                    self._index(rec)

    def get(
        self,
        dataset_id: str,
        rule_id: str,
        rule_version: int,
        shard: str,
        dataset_digest: str | None = None,
    ) -> dict | None:
        """读取指定键的既有结论，不存在返回 None。

        不传 dataset_digest 时退化为按 (数据集,规则,版本,分片) 取最新索引记录，
        主要供 rerun_failed_only 复用；建议监控场景显式传入内容指纹。
        """
        if dataset_digest is not None:
            return self._records.get(
                (dataset_id, dataset_digest, rule_id, rule_version, shard))
        for rec in self._records.values():
            if (
                rec["dataset_id"] == dataset_id
                and rec["rule_id"] == rule_id
                and rec["rule_version"] == rule_version
                and rec["shard"] == shard
            ):
                return rec
        return None

    def query(
        self,
        dataset_id: str | None = None,
        rule_id: str | None = None,
        rule_version: int | None = None,
        dataset_digest: str | None = None,
        run_id: str | None = None,
    ) -> list[dict]:
        """按数据集/内容版本/规则/规则版本/运行过滤历史结论。"""
        out = []
        for rec in self._records.values():
            if dataset_id is not None and rec["dataset_id"] != dataset_id:
                continue
            if dataset_digest is not None and rec["dataset_digest"] != dataset_digest:
                continue
            if rule_id is not None and rec["rule_id"] != rule_id:
                continue
            if rule_version is not None and rec["rule_version"] != rule_version:
                continue
            if run_id is not None and rec["run_id"] != run_id:
                continue
            out.append(rec)
        return sorted(out, key=self._sort_key)

    @staticmethod
    def _sort_key(r: dict) -> tuple:
        return (
            r["dataset_id"], r["dataset_digest"], r["rule_id"],
            r["rule_version"], r["shard"],
        )

    def list_digests(self, dataset_id: str) -> list[str]:
        """按首次出现顺序返回数据集历史跑过的所有数据内容指纹。"""
        return list(self._digest_order.get(dataset_id, ()))

    def query_runs(self, dataset_id: str | None = None) -> list[dict]:
        """按运行标识聚合，返回每次运行的摘要（含内容指纹与终态计数）。"""
        groups: dict[str, dict] = {}
        order: list[str] = []
        for rec in self._records.values():
            if dataset_id is not None and rec["dataset_id"] != dataset_id:
                continue
            rid = rec["run_id"]
            g = groups.get(rid)
            if g is None:
                g = {
                    "run_id": rid,
                    "dataset_id": rec["dataset_id"],
                    "dataset_digest": rec["dataset_digest"],
                    "total": 0,
                    "passed": 0,
                    "failed": 0,
                    "error": 0,
                    "rejected": 0,
                }
                groups[rid] = g
                order.append(rid)
            g["total"] += 1
            g[rec["status"]] = g.get(rec["status"], 0) + 1
        return [groups[rid] for rid in order]
