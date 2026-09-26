"""服务端到端：运行校验、按版本查询、部分重跑。"""
import importlib
import logging
import os

import pytest
from fastapi.testclient import TestClient

logger = logging.getLogger("test.service")


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DQ_STORE_PATH", str(tmp_path / "results.jsonl"))
    import main
    importlib.reload(main)
    return TestClient(main.app)


RULES = [
    {"rule_id": "amount-range", "rule_type": "value_range", "version": 1,
     "params": {"field": "amount", "min": 0}},
    {"rule_id": "no-dup", "rule_type": "duplicate_record", "version": 1,
     "params": {"keys": ["order_id"]}},
]


def test_run_and_query_by_version(client):
    r = client.post("/checks/run", json={"dataset_id": "orders", "rules": RULES})
    assert r.status_code == 200
    body = r.json()
    logger.info("verdict=%s results=%s", body["verdict"],
                [(x["rule_id"], x["status"]) for x in body["results"]])
    assert body["verdict"] == "failed"

    r = client.get("/checks/results",
                   params={"dataset_id": "orders", "rule_id": "amount-range",
                           "rule_version": 1})
    assert r.status_code == 200 and len(r.json()) == 1

    # 规则升级版本重跑，历史 v1 结论仍在
    rules_v2 = [dict(RULES[0], version=2, params={"field": "amount", "min": -10}),
                RULES[1]]
    r = client.post("/checks/run", json={"dataset_id": "orders", "rules": rules_v2})
    assert r.status_code == 200
    r = client.get("/checks/results",
                   params={"dataset_id": "orders", "rule_id": "amount-range"})
    versions = sorted(x["rule_version"] for x in r.json())
    assert versions == [1, 2]


def test_partial_rerun_via_api(client):
    bad = {"rule_id": "bad-rule", "rule_type": "value_range", "version": 1,
           "params": {"field": "amount"}}
    r = client.post("/checks/run", json={"dataset_id": "orders", "rules": RULES + [bad]})
    assert r.json()["verdict"] == "partial"

    fixed = dict(bad, version=2, params={"field": "amount", "min": -10})
    r = client.post("/checks/run", json={
        "dataset_id": "orders", "rules": RULES + [fixed],
        "only_rule_ids": ["bad-rule"]})
    body = r.json()
    logger.info("rerun verdict=%s", body["verdict"])
    by_id = {x["rule_id"]: x for x in body["results"]}
    assert by_id["bad-rule"]["status"] == "passed"
    assert by_id["bad-rule"]["rule_version"] == 2
    assert body["verdict"] == "failed"  # 其余真实失败仍在


def test_missing_dataset_404(client):
    r = client.post("/checks/run", json={"dataset_id": "nope", "rules": RULES})
    assert r.status_code == 404
