"""结论留痕存储：追加写、按内容版本与规则版本查询、不覆盖既有结论。

存储格式为 JSONL，每行一条 RuleResult 记录，附带数据集标识与内容指纹。
键为 (dataset_id, dataset_digest, rule_id, rule_version, shard)：
内容指纹入键意味着——同一数据集内容变化后会形成新的内容版本，新旧版本结论
各自独立留痕、互不覆盖，也不会因为“同数据集、同规则版本”而报错；同一份
数据内容在同一规则版本下重跑则幂等收敛。

- 键不存在 -> 追加写入；
- 键已存在且内容完全一致 -> 幂等 no-op（相同输入的结论可复现，
  重复记录相同结论不构成篡改）；
- 键已存在、内容不一致、且既有结论为终态结论（passed/failed）
  -> 拒绝（DuplicateResultError），既不篡改也不覆盖；
- 键已存在但既有记录为非终态（error/rejected，表示"未得出结论"）
  -> 允许追加新记录并取代索引（文件仍为追加写，旧记录留痕可通过
  query_history 查到，重跑恢复后结论可收敛，且历史失败痕迹不丢失）。
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

from dqgate.models import RuleResult


class DuplicateResultError(RuntimeError):
    """同一 (数据集, 内容版本, 规则, 版本, 分片) 已存在不一致的终态结论，拒绝覆盖。"""


_NON_TERMINAL = {"error", "rejected"}  # 非终态：未得出结论，允许重跑后取代


class ResultStore:
    """基于 JSONL 的追加式结果存储（线程安全）。"""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._records: dict[tuple, dict] = {}   # 每个键的最新结论（索引）
        self._history: list[dict] = []          # 全量追加留痕（含被取代的非终态记录）
        if self._path.exists():
            for rec in self._read_all():
                self._history.append(rec)
                key = self._key_of(rec)
                old = self._records.get(key)
                # 与 append 的取代规则一致：非终态记录可被后写入记录取代索引
                if old is None or old["status"] in _NON_TERMINAL:
                    self._records[key] = rec

    @staticmethod
    def _key_of(rec: dict) -> tuple:
        return (
            rec["dataset_id"],
            rec["dataset_digest"],
            rec["rule_id"],
            rec["rule_version"],
            rec["shard"],
        )

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
                    self._history.append(rec)
                    self._records[self._key_of(rec)] = rec

    def get(
        self,
        dataset_id: str,
        dataset_digest: str,
        rule_id: str,
        rule_version: int,
        shard: str,
    ) -> dict | None:
        """读取指定 (数据集, 内容版本, 规则, 规则版本, 分片) 的最新结论，不存在返回 None。"""
        return self._records.get((dataset_id, dataset_digest, rule_id, rule_version, shard))

    def query(
        self,
        dataset_id: str | None = None,
        rule_id: str | None = None,
        rule_version: int | None = None,
        *,
        dataset_digest: str | None = None,
    ) -> list[dict]:
        """按数据集/内容版本/规则/规则版本过滤查询最新结论。

        不传 dataset_digest 时返回该数据集所有内容版本的结论
        （内容版本之间互不覆盖、可分别查出）。dataset_digest 仅以关键字传入，
        以兼容旧的 (dataset_id, rule_id, rule_version) 位置参数调用。
        """
        out = [
            rec for rec in self._records.values()
            if _matches(rec, dataset_id, dataset_digest, rule_id, rule_version)
        ]
        return sorted(
            out,
            key=lambda r: (
                r["dataset_id"], r["dataset_digest"], r["rule_id"],
                r["rule_version"], r["shard"],
            ),
        )

    def query_history(
        self,
        dataset_id: str | None = None,
        rule_id: str | None = None,
        rule_version: int | None = None,
        *,
        dataset_digest: str | None = None,
    ) -> list[dict]:
        """查询全量追加留痕（含被重跑取代的 error/rejected 历史记录）。"""
        return [
            rec for rec in self._history
            if _matches(rec, dataset_id, dataset_digest, rule_id, rule_version)
        ]

    def list_digests(self, dataset_id: str) -> list[str]:
        """列出某数据集已有留痕的全部内容版本指纹（按首次出现顺序）。"""
        seen: list[str] = []
        for rec in self._history:
            if rec["dataset_id"] == dataset_id and rec["dataset_digest"] not in seen:
                seen.append(rec["dataset_digest"])
        return seen


def _matches(
    rec: dict,
    dataset_id: str | None,
    dataset_digest: str | None,
    rule_id: str | None,
    rule_version: int | None,
) -> bool:
    if dataset_id is not None and rec["dataset_id"] != dataset_id:
        return False
    if dataset_digest is not None and rec["dataset_digest"] != dataset_digest:
        return False
    if rule_id is not None and rec["rule_id"] != rule_id:
        return False
    if rule_version is not None and rec["rule_version"] != rule_version:
        return False
    return True
