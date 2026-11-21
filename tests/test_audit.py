"""盘点 API 的端到端测试：真实 HTTP 栈（httpx ASGI transport）打到 mockkube。

覆盖：

* 分页收齐后才写 /status；只计属主 UID 匹配、ready、未删除、带 target 注解的快照；
* 各目标 available / retention / gap，generation 与列表 resourceVersion 都进状态；
* status 其余字段保留；v1beta1 与 v1alpha1 两个版本都能读到结果；
* 续页过期（410）、读页失败 -> 保留上次结果 + auditError；
* 盘点中改策略 -> 用同一份清单按现行策略重算，不重扫；
* 策略同名重建 / 另一轮盘点已更新 -> 拒绝写回；
* 写 /status 冲突 -> 拒绝写回，原状态不变。
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone

import pytest

from app import mockkube
from app.audit import audit_policy
from app.kube import KubernetesClient, KubernetesError

NOW = datetime(2026, 11, 21, 8, 30, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def fresh_mock():
    mockkube.reset_state()
    yield
    mockkube.reset_state()


def kube_client(mock_server: str) -> KubernetesClient:
    return KubernetesClient(mock_server)


def stored_policy(namespace: str = mockkube.DEMO_NAMESPACE, name: str = mockkube.DEMO_POLICY) -> dict:
    with mockkube.LOCK:
        return copy.deepcopy(mockkube.STATE.policies[(namespace, name)])


def audit(mock_server: str, page_size: int = 50) -> dict:
    with kube_client(mock_server) as client:
        return audit_policy(client, mockkube.DEMO_NAMESPACE, mockkube.DEMO_POLICY,
                            page_size=page_size, now=NOW)


def by_name(targets: list[dict]) -> dict[str, dict]:
    return {target["name"]: target for target in targets}


# ---------------------------------------------------------------------------
# 正常盘点
# ---------------------------------------------------------------------------

def test_audit_counts_targets_and_writes_status(mock_server):
    result = audit(mock_server)

    assert result["outcome"] == "written"
    assert result["recomputed"] is False
    assert result["policyGeneration"] == 7
    assert result["snapshotResourceVersion"] == "5000"

    targets = by_name(result["audit"]["targets"])
    # db 保留 5，可用 2（其它属主 / 未 ready / 终止中都不计）
    assert targets["db"] == {"name": "db", "retention": 5, "available": 2, "gap": 3}
    # cache 显式 3，恰好 3 份可用
    assert targets["cache"] == {"name": "cache", "retention": 3, "available": 3, "gap": 0}
    # logs 未显式设置 -> 跟随默认 3，仅 1 份
    assert targets["logs"] == {"name": "logs", "retention": 3, "available": 1, "gap": 2}
    # metrics 保留 2，零份
    assert targets["metrics"] == {"name": "metrics", "retention": 2, "available": 0, "gap": 2}
    # 非策略目标（ghost）与缺注解快照不计入
    assert set(targets) == {"db", "cache", "logs", "metrics"}

    status = stored_policy()["status"]
    assert status["audit"]["runID"] == result["audit"]["runID"]
    assert status["audit"]["policyUID"] == mockkube.DEMO_POLICY_UID
    assert status["audit"]["generation"] == 7
    assert status["audit"]["snapshotResourceVersion"] == "5000"
    assert status["audit"]["completedAt"] == "2026-11-21T08:30:00Z"
    assert status["audit"]["targets"] == result["audit"]["targets"]


def test_audit_preserves_other_status_fields(mock_server):
    before = stored_policy()["status"]
    assert before["observedGeneration"] == 7  # 前置条件
    audit(mock_server)
    after = stored_policy()["status"]
    assert after["observedGeneration"] == 7
    assert after["message"] == "controller heartbeat ok"
    assert after["conditions"] == before["conditions"]


def test_audit_result_readable_from_both_versions(mock_server):
    result = audit(mock_server)
    with kube_client(mock_server) as client:
        v1beta1 = client.get_policy(mockkube.DEMO_NAMESPACE, mockkube.DEMO_POLICY)
        v1alpha1 = client._request(
            "GET",
            "/apis/backup.example.com/v1alpha1/namespaces/default/backuppolicies/web-backup",
        )
    assert v1beta1["status"]["audit"]["targets"] == result["audit"]["targets"]
    assert v1alpha1["status"]["audit"]["targets"] == result["audit"]["targets"]
    assert v1alpha1["status"]["audit"]["generation"] == 7
    # 旧客户端视图的 spec 仍是摊平后的目标名列表
    assert v1alpha1["spec"]["targets"] == ["db", "cache", "logs", "metrics"]


@pytest.mark.parametrize("page_size", [1, 2, 3, 5, 50])
def test_pagination_collects_single_inventory(mock_server, page_size):
    result = audit(mock_server, page_size=page_size)
    assert by_name(result["audit"]["targets"])["db"]["available"] == 2
    assert by_name(result["audit"]["targets"])["cache"]["available"] == 3
    snapshot_count = len(mockkube.STATE.snapshots[mockkube.DEMO_NAMESPACE])
    assert mockkube.STATE.list_requests == -(-snapshot_count // page_size)
    assert result["snapshotResourceVersion"] == "5000"


def test_audit_returns_404_for_missing_policy(mock_server):
    from app.audit import BackupPolicyNotFound
    with kube_client(mock_server) as client, pytest.raises(BackupPolicyNotFound):
        audit_policy(client, "default", "does-not-exist")


# ---------------------------------------------------------------------------
# 失败：保留上次结果，只写 auditError
# ---------------------------------------------------------------------------

def test_expired_continue_token_keeps_previous_result(mock_server):
    first = audit(mock_server, page_size=2)
    # 下一次盘点每页收 2 个 -> 第 1 页之后作废 token
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "expire_continue_tokens",
            "params": {},
            "fired": False,
        }
    )
    stale = audit(mock_server, page_size=2)

    assert stale["outcome"] == "stale"
    assert stale["error"]["reason"] == "Expired"
    assert "continue token" in stale["error"]["message"]
    assert stale["audit"] == first["audit"]  # 上一次结果原样保留
    status = stored_policy()["status"]
    assert status["audit"] == first["audit"]
    assert status["auditError"]["reason"] == "Expired"
    assert status["observedGeneration"] == 7  # 其它字段仍在


def test_successful_audit_clears_previous_error(mock_server):
    assert audit(mock_server)["outcome"] == "written"
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "expire_continue_tokens",
            "params": {},
            "fired": False,
        }
    )
    assert audit(mock_server, page_size=2)["outcome"] == "stale"
    assert stored_policy()["status"]["auditError"]["reason"] == "Expired"

    refreshed = audit(mock_server)
    assert refreshed["outcome"] == "written"
    status = stored_policy()["status"]
    assert "auditError" not in status
    assert status["audit"]["runID"] == refreshed["audit"]["runID"]


def test_read_failure_keeps_previous_result_and_reports_reason(mock_server):
    """续页读取失败（非过期）：同样保留上次结果，报出底层原因。"""
    first = audit(mock_server, page_size=50)
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "fail_continue_page",
            "params": {},
            "fired": False,
        }
    )
    stale = audit(mock_server, page_size=2)
    assert stale["outcome"] == "stale"
    assert stale["error"]["reason"] == "InternalError"
    assert "etcdserver" in stale["error"]["message"]
    assert stale["audit"]["runID"] == first["audit"]["runID"]
    status = stored_policy()["status"]
    assert status["audit"]["runID"] == first["audit"]["runID"]
    assert status["auditError"]["reason"] == "InternalError"


def test_transport_failure_reports_connection_reason(mock_server, monkeypatch):
    first = audit(mock_server)

    def boom(*args, **kwargs):
        raise KubernetesError("KubernetesAPIConnectionFailed", "connection refused")

    with kube_client(mock_server) as client:
        # 策略能读到，但读快照时连接失败：应保留上次结果并报原因
        monkeypatch.setattr(client, "list_volumesnapshots", boom)
        stale = audit_policy(client, mockkube.DEMO_NAMESPACE, mockkube.DEMO_POLICY, now=NOW)

    assert stale["outcome"] == "stale"
    assert stale["error"]["reason"] == "KubernetesAPIConnectionFailed"
    assert "connection refused" in stale["error"]["message"]
    assert stale["audit"] == first["audit"]
    status = stored_policy()["status"]
    assert status["audit"]["runID"] == first["audit"]["runID"]
    assert status["auditError"]["reason"] == "KubernetesAPIConnectionFailed"


def test_inconsistent_page_versions_abort_instead_of_mixing_lists(mock_server):
    """续页 RV 与首屏不一致 -> 不把两批快照当成同一份清单。"""
    first = audit(mock_server)
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "inconsistent_pages",
            "params": {},
            "fired": False,
        }
    )
    stale = audit(mock_server, page_size=2)
    assert stale["outcome"] == "stale"
    assert stale["error"]["reason"] == "InconsistentListVersion"
    assert "resourceVersion" in stale["error"]["message"]
    assert stored_policy()["status"]["audit"]["runID"] == first["audit"]["runID"]


# ---------------------------------------------------------------------------
# 盘点中策略变化
# ---------------------------------------------------------------------------

def test_policy_changed_mid_audit_recomputes_without_rescan(mock_server):
    # 第 1 页服务完后把默认值改成 6、给 metrics 单独设 4
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "bump_generation",
            "params": {
                "spec": {
                    "defaultRetention": 6,
                    "targets": [
                        {"name": "db", "retention": 5},
                        {"name": "cache", "retention": 3},
                        {"name": "logs"},
                        {"name": "metrics", "retention": 4},
                    ],
                }
            },
            "fired": False,
        }
    )
    result = audit(mock_server, page_size=2)

    assert result["outcome"] == "written"
    assert result["recomputed"] is True
    assert result["policyGeneration"] == 8
    # 只扫了收齐清单所需的两页半：12 个 / 每页 2 = 6 页，且没有重扫
    assert mockkube.STATE.list_requests == 6

    targets = by_name(result["audit"]["targets"])
    assert targets["logs"]["retention"] == 6          # 跟随新默认
    assert targets["logs"]["gap"] == 5               # 6 - 1
    assert targets["metrics"]["retention"] == 4
    assert targets["cache"]["gap"] == 0
    anomaly = stored_policy()["status"]["audit"]
    assert anomaly["generation"] == 8
    assert anomaly["snapshotResourceVersion"] == "5000"  # 清单仍来自同一份


def test_policy_recreated_mid_audit_is_rejected(mock_server):
    before = stored_policy()
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "recreate_policy",
            "params": {},
            "fired": False,
        }
    )
    from app.audit import AuditRejected

    with kube_client(mock_server) as client, pytest.raises(AuditRejected) as info:
        audit_policy(client, mockkube.DEMO_NAMESPACE, mockkube.DEMO_POLICY, page_size=2)

    assert info.value.reason == "PolicyRecreated"
    after = stored_policy()
    assert mockkube.STATE.status_puts == 0              # 没有写回
    assert mockkube.STATE.status_put_attempts == 0
    assert after["metadata"]["uid"] != before["metadata"]["uid"]


def test_policy_deleted_mid_audit_is_rejected(mock_server):
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "delete_policy",
            "params": {},
            "fired": False,
        }
    )
    from app.audit import AuditRejected

    with kube_client(mock_server) as client, pytest.raises(AuditRejected) as info:
        audit_policy(client, mockkube.DEMO_NAMESPACE, mockkube.DEMO_POLICY, page_size=2)

    assert info.value.reason == "PolicyDeleted"
    assert mockkube.STATE.status_puts == 0


def test_another_audit_updated_result_mid_audit_is_rejected(mock_server):
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "set_audit_run_id",
            "params": {"runID": "another-run"},
            "fired": False,
        }
    )
    from app.audit import AuditRejected

    with kube_client(mock_server) as client, pytest.raises(AuditRejected) as info:
        audit_policy(client, mockkube.DEMO_NAMESPACE, mockkube.DEMO_POLICY, page_size=2)

    assert info.value.reason == "ConcurrentAuditUpdate"
    assert stored_policy()["status"].get("audit") == {"runID": "another-run"}


def test_status_write_conflict_is_rejected(mock_server):
    before = stored_policy()
    with mockkube.LOCK:
        mockkube.STATE.status_conflict = True
    from app.audit import AuditRejected

    with kube_client(mock_server) as client, pytest.raises(AuditRejected) as info:
        audit_policy(client, mockkube.DEMO_NAMESPACE, mockkube.DEMO_POLICY)

    assert info.value.reason == "ConcurrentAuditUpdate"
    assert stored_policy()["status"] == before["status"]


# ---------------------------------------------------------------------------
# 过滤规则：逐条验证"只计满足条件的快照"
# ---------------------------------------------------------------------------

def _single_snapshot(**kwargs) -> dict:
    target = kwargs.pop("target", "db")
    return mockkube._snapshot("only", target, **kwargs)


@pytest.mark.parametrize(
    "snapshot, counted",
    [
        (_single_snapshot(), True),
        (_single_snapshot(owner_uid="other"), False),
        (_single_snapshot(ready=False), False),
        (_single_snapshot(deleting=True), False),
        (_single_snapshot(target=None), False),
    ],
)
def test_snapshot_filter_rules(snapshot, counted):
    from app.audit import _count_available

    assert _count_available([snapshot], mockkube.DEMO_POLICY_UID) == (
        {"db": 1} if counted else {}
    )


def test_snapshot_without_status_or_owner_references_is_ignored():
    from app.audit import _count_available

    snapshot = _single_snapshot()
    snapshot.pop("status")
    assert _count_available([snapshot], mockkube.DEMO_POLICY_UID) == {}

    snapshot = _single_snapshot()
    snapshot["metadata"].pop("ownerReferences")
    assert _count_available([snapshot], mockkube.DEMO_POLICY_UID) == {}


def test_owner_uid_comes_from_policy_not_name():
    """同名但 UID 不同的属主（重建前的旧对象）不能被计入。"""
    from app.audit import _count_available

    snapshot = _single_snapshot()
    snapshot["metadata"]["ownerReferences"][0]["name"] = mockkube.DEMO_POLICY
    assert _count_available([snapshot], "stale-uid") == {}


def test_pagination_counts_all_items_across_pages(mock_server):
    """同一份清单跨页时逐页累积，不因分页边界丢份数。"""
    snapshots = [_single_snapshot() for _ in range(5)]
    for index, snapshot in enumerate(snapshots):
        snapshot["metadata"]["name"] = f"only-{index}"
        snapshot["metadata"]["uid"] = f"uid-{index}"
    mockkube.reset_state(snapshots={mockkube.DEMO_NAMESPACE: snapshots})

    result = audit(mock_server, page_size=2)
    assert by_name(result["audit"]["targets"])["db"]["available"] == 5
    assert mockkube.STATE.list_requests == 3
