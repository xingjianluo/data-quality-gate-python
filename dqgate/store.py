"""结论留痕存储：追加写、按版本查询、不覆盖既有结论。

存储格式为 JSONL，每行一条 RuleResult 记录，附带数据集标识与内容指纹。
键为 (dataset_id, rule_id, rule_version, shard)：
- 键不存在 -> 追加写入；
- 键已存在且内容完全一致 -> 幂等 no-op（同一数据集同一规则版本的结论可复现，
  重复记录相同结论不构成篡改）；
- 键已存在、内容不一致、且既有结论为终态结论（passed/failed）
  -> 拒绝（DuplicateResultError），既不篡改也不覆盖；
- 键已存在但既有记录为非终态（error/rejected，表示"未得出结论"）
  -> 允许追加新记录并取代索引（文件仍为追加写，旧记录留痕可查，
  重跑恢复后结论可收敛，且历史失败痕迹不丢失）。
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

from dqgate.models import RuleResult


class DuplicateResultError(RuntimeError):
    """同一 (数据集, 规则, 版本, 分片) 已存在不一致的终态结论，拒绝覆盖。"""


_NON_TERMINAL = {"error", "rejected"}  # 非终态：未得出结论，允许重跑后取代


class ResultStore:
    """基于 JSONL 的追加式结果存储（线程安全）。"""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._records: dict[tuple, dict] = {}
        if self._path.exists():
            for rec in self._read_all():
                self._records[self._key_of(rec)] = rec

    @staticmethod
    def _key_of(rec: dict) -> tuple:
        return (rec["dataset_id"], rec["rule_id"], rec["rule_version"], rec["shard"])

    def _read_all(self) -> list[dict]:
        out = []
        with self._path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out

    def append(self, dataset_id: str, dataset_digest: str, results: list[RuleResult]) -> None:
        """追加一批结果；相同键相同内容为幂等 no-op，内容冲突则整批拒绝。"""
        records = [
            {
                "dataset_id": dataset_id,
                "dataset_digest": dataset_digest,
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
                    and existing != rec
                    and existing["status"] not in _NON_TERMINAL
                ):
                    raise DuplicateResultError(f"结论已存在且内容不一致，拒绝覆盖: {key}")
                seen[key] = rec
            to_write = [rec for key, rec in seen.items() if self._records.get(key) != rec]
            if to_write:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                with self._path.open("a", encoding="utf-8") as f:
                    for rec in to_write:
                        f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")
                for rec in to_write:
                    self._records[self._key_of(rec)] = rec

    def get(self, dataset_id: str, rule_id: str, rule_version: int, shard: str) -> dict | None:
        """读取指定键的既有结论，不存在返回 None。"""
        return self._records.get((dataset_id, rule_id, rule_version, shard))

    def query(
        self,
        dataset_id: str | None = None,
        rule_id: str | None = None,
        rule_version: int | None = None,
    ) -> list[dict]:
        """按数据集/规则/版本过滤查询历史结论（不同版本可区分）。"""
        out = []
        for rec in self._records.values():
            if dataset_id is not None and rec["dataset_id"] != dataset_id:
                continue
            if rule_id is not None and rec["rule_id"] != rule_id:
                continue
            if rule_version is not None and rec["rule_version"] != rule_version:
                continue
            out.append(rec)
        return sorted(out, key=lambda r: (r["dataset_id"], r["rule_id"], r["rule_version"], r["shard"]))
