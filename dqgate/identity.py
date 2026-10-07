"""确定性指纹：同一份数据内容 + 同一套规则版本 -> 同一个运行标识（可复现）。

run_id 只取决于数据内容指纹、规则集合（id/类型/版本/参数）与分片数，
与运行时刻、进程、主机无关；隔多久跑跑、重启后再跑，标识与结论都一致。
"""

from __future__ import annotations

import hashlib
import json

from dqgate.models import RuleDefinition


def _canonical(obj: object) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def rules_fingerprint(rules: list[RuleDefinition], num_shards: int) -> str:
    """对规则集合（含类型/版本/参数）与分片数计算稳定指纹。

    规则按 rule_id 排序后参与哈希，因此传入顺序不影响指纹；
    任一规则的类型、版本或参数变化都会得到不同指纹。
    """
    payload = [
        {
            "rule_id": r.rule_id,
            "rule_type": r.rule_type.value,
            "version": r.version,
            "params": r.params,
        }
        for r in sorted(rules, key=lambda r: r.rule_id)
    ]
    h = hashlib.sha256()
    h.update(_canonical({"rules": payload, "num_shards": num_shards}).encode("utf-8"))
    return h.hexdigest()


def deterministic_run_id(
    dataset_id: str,
    dataset_digest: str,
    rules: list[RuleDefinition],
    num_shards: int,
) -> str:
    """由数据集标识+内容指纹+规则集合指纹派生确定性运行标识。

    形式为 run-<12 位哈希>，只取决于输入内容，不依赖时间或随机数。
    """
    material = {
        "dataset_id": dataset_id,
        "dataset_digest": dataset_digest,
        "rules_fingerprint": rules_fingerprint(rules, num_shards),
    }
    digest = hashlib.sha256(_canonical(material).encode("utf-8")).hexdigest()
    return f"run-{digest[:12]}"
