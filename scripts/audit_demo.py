#!/usr/bin/env python3
"""对运行中的盘点 API 实测全流程（Compose 场景）。

用法：
    python3 scripts/audit_demo.py [API_URL] [MOCK_URL]
    # 默认 http://127.0.0.1:8080 与 http://127.0.0.1:8081

覆盖：
1. 正常盘点：分页收齐 -> 各目标 available / gap -> /status 写入；
2. 两个版本读取同一份盘点结果；
3. 续页过期：保留上次结果，auditError 报 Expired；
4. 盘点中改策略：同一份清单按新策略重算（不重扫），generation 更新；
5. 同名重建 / 另一轮盘点已更新：409 拒绝写回；
6. 状态写回冲突：409 拒绝写回。
"""

from __future__ import annotations

import json
import sys

import httpx

API = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080"
MOCK = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8081"

POLICY = "/apis/backup.example.com/{version}/namespaces/default/backuppolicies/web-backup"


def show(title: str, obj) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def reset() -> None:
    httpx.post(f"{MOCK}/_mock/reset", json={}, timeout=10).raise_for_status()


def audit(**params) -> httpx.Response:
    return httpx.post(f"{API}/audit/default/web-backup", params=params, timeout=30)


def schedule(action: str, after: int, **params) -> None:
    response = httpx.post(
        f"{MOCK}/_mock/schedule",
        json={"action": action, "afterListRequest": after, "params": params},
        timeout=10,
    )
    response.raise_for_status()


def read_policy(version: str = "v1beta1") -> dict:
    return httpx.get(f"{MOCK}{POLICY.format(version=version)}", timeout=10).json()


def targets_of(body: dict) -> dict[str, dict]:
    return {t["name"]: t for t in body["audit"]["targets"]}


def main() -> None:
    print(f"audit API: {API}\nmock kube: {MOCK}")
    assert httpx.get(f"{API}/healthz", timeout=5).json() == {"status": "ok"}

    # ---- 1. 正常盘点（page_size=2 强制分页）----
    reset()
    response = audit(page_size=2)
    assert response.status_code == 200, response.text
    body = response.json()
    show("1. 盘点结果（分页 2，收齐整份清单后写 /status）", body)
    assert body["outcome"] == "written"
    assert body["snapshotResourceVersion"] == "5000"
    assert body["recomputed"] is False
    targets = targets_of(body)
    assert targets["db"] == {"name": "db", "retention": 5, "available": 2, "gap": 3}
    assert targets["cache"]["gap"] == 0
    assert targets["logs"]["gap"] == 2          # 默认 3 - 可用 1
    assert targets["metrics"]["gap"] == 2
    assert "ghost" not in targets and "untagged" not in targets

    stored = read_policy()["status"]
    assert stored["audit"]["generation"] == 7
    assert stored["audit"]["targets"] == body["audit"]["targets"]
    # 其他状态字段保留
    assert stored["observedGeneration"] == 7
    assert stored["message"] == "controller heartbeat ok"
    assert stored["conditions"][0]["type"] == "Ready"

    # ---- 2. 两个版本都能读到 ----
    old = read_policy("v1alpha1")
    assert old["status"]["audit"] == stored["audit"]
    assert old["status"]["audit"]["snapshotResourceVersion"] == "5000"
    show("2. 旧客户端（v1alpha1）读到的盘点结果", old["status"]["audit"])

    # ---- 3. 续页过期：保留上次结果 ----
    previous_run = body["audit"]["runID"]
    schedule("expire_continue_tokens", 1)
    stale = audit(page_size=2)
    show("3. 续页过期（410）：保留上次结果并报原因", stale.json())
    assert stale.status_code == 200
    stale_body = stale.json()
    assert stale_body["outcome"] == "stale"
    assert stale_body["error"]["reason"] == "Expired"
    assert "continue token" in stale_body["error"]["message"]
    assert stale_body["audit"]["runID"] == previous_run
    after_stale = read_policy()["status"]
    assert after_stale["audit"]["runID"] == previous_run
    assert after_stale["auditError"]["reason"] == "Expired"

    # ---- 4. 盘点中改策略：同一份清单按现行策略重算 ----
    reset()
    schedule(
        "bump_generation",
        1,
        spec={
            "defaultRetention": 6,
            "targets": [
                {"name": "db", "retention": 5},
                {"name": "cache", "retention": 3},
                {"name": "logs"},
                {"name": "metrics", "retention": 4},
            ],
        },
    )
    changed = audit(page_size=2).json()
    show("4. 盘点中把默认值改到 6、metrics 设 4：按现行策略重算", changed)
    assert changed["outcome"] == "written"
    assert changed["recomputed"] is True
    assert changed["policyGeneration"] == 8
    targets = targets_of(changed)
    assert targets["logs"]["retention"] == 6 and targets["logs"]["gap"] == 5
    assert targets["metrics"]["retention"] == 4 and targets["metrics"]["gap"] == 4
    assert changed["snapshotResourceVersion"] == "5000"      # 没有重扫，清单 RV 不变

    # ---- 5a. 同名重建 -> 拒绝 ----
    reset()
    schedule("recreate_policy", 1)
    rejected = audit(page_size=2)
    show("5a. 盘点中策略同名重建：拒绝写回", rejected.json())
    assert rejected.status_code == 409
    assert rejected.json()["error"]["reason"] == "PolicyRecreated"
    assert read_policy()["status"] == {}

    # ---- 5b. 另一轮盘点已更新结果 -> 拒绝 ----
    reset()
    schedule("set_audit_run_id", 1, runID="another-run")
    rejected = audit(page_size=2)
    show("5b. 另一轮盘点已更新 status.audit：拒绝写回", rejected.json())
    assert rejected.status_code == 409
    assert rejected.json()["error"]["reason"] == "ConcurrentAuditUpdate"
    assert read_policy()["status"]["audit"] == {"runID": "another-run"}

    # ---- 6. 状态写回冲突 -> 拒绝 ----
    reset()
    httpx.post(f"{MOCK}/_mock/status-conflict", json={"enabled": True}, timeout=5).raise_for_status()
    rejected = audit(page_size=2)
    show("6. 写 /status 遇到 409：拒绝覆盖", rejected.json())
    assert rejected.status_code == 409
    assert "modified by another writer" in rejected.json()["error"]["message"]

    reset()
    print("\n全部断言通过 ✅")


if __name__ == "__main__":
    main()
