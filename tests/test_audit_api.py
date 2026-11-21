"""HTTP 层测试：``POST /audit/{namespace}/{name}`` 的响应与错误映射。

mockkube 仍跑在真实 uvicorn 线程里（见 conftest），这里通过改写
``app.main.KUBERNETES_API_URL`` 把盘点 API 指向它。
"""

from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from app import main, mockkube

client = TestClient(main.app)


@pytest.fixture(autouse=True)
def point_at_mock(mock_server, monkeypatch):
    monkeypatch.setattr(main, "KUBERNETES_API_URL", mock_server)
    mockkube.reset_state()
    yield
    mockkube.reset_state()


def audit_http(namespace: str = "default", name: str = "web-backup", **params) -> dict:
    response = client.post(f"/audit/{namespace}/{name}", params=params)
    return response


def test_audit_endpoint_writes_status(mock_server):
    response = audit_http(page_size=2)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["outcome"] == "written"
    assert body["snapshotResourceVersion"] == "5000"
    assert {t["name"]: t["gap"] for t in body["audit"]["targets"]} == {
        "db": 3,
        "cache": 0,
        "logs": 2,
        "metrics": 2,
    }

    # 旧版 API 也能读到同一份结果
    old = httpx.get(
        f"{mock_server}/apis/backup.example.com/v1alpha1/namespaces/default/backuppolicies/web-backup"
    ).json()
    assert old["status"]["audit"]["targets"] == body["audit"]["targets"]


def test_audit_endpoint_404():
    response = audit_http(name="missing")
    assert response.status_code == 404
    assert response.json()["error"]["reason"] == "BackupPolicyNotFound"


def test_audit_endpoint_409_on_policy_recreate(mock_server):
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "recreate_policy",
            "params": {},
            "fired": False,
        }
    )
    response = audit_http(page_size=2)
    assert response.status_code == 409
    assert response.json()["error"]["reason"] == "PolicyRecreated"
    assert mockkube.STATE.status_puts == 0


def test_audit_endpoint_409_on_concurrent_audit(mock_server):
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "set_audit_run_id",
            "params": {"runID": "other-run"},
            "fired": False,
        }
    )
    response = audit_http(page_size=2)
    assert response.status_code == 409
    assert response.json()["error"]["reason"] == "ConcurrentAuditUpdate"


def test_audit_endpoint_409_on_status_conflict(mock_server):
    with mockkube.LOCK:
        mockkube.STATE.status_conflict = True
    response = audit_http()
    assert response.status_code == 409
    assert "modified by another writer" in response.json()["error"]["message"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda spec: spec.update(targets=[{"name": "db", "retention": -1}]),
        lambda spec: spec.update(targets=["db"]),
        lambda spec: spec.pop("defaultRetention"),
    ],
    ids=["negative-retention", "targets-not-objects", "missing-default"],
)
def test_audit_endpoint_422_on_invalid_policy(mock_server, mutate):
    with mockkube.LOCK:
        stored = mockkube.STATE.policies[("default", "web-backup")]
        mutate(stored["spec"])
    response = audit_http()
    assert response.status_code == 422, response.text
    assert response.json()["error"]["reason"] == "InvalidBackupPolicy"
    assert mockkube.STATE.status_puts == 0


def test_audit_endpoint_502_when_kubernetes_unreachable(monkeypatch):
    monkeypatch.setattr(main, "KUBERNETES_API_URL", "http://127.0.0.1:1")
    response = audit_http()
    assert response.status_code == 502
    assert response.json()["error"]["reason"] == "KubernetesAPIConnectionFailed"


def test_audit_endpoint_stale_is_still_200(mock_server):
    assert audit_http().status_code == 200
    mockkube.STATE.scheduled.append(
        {
            "after_list_request": 1,
            "scheduled_at": mockkube.STATE.list_requests,
            "action": "expire_continue_tokens",
            "params": {},
            "fired": False,
        }
    )
    response = audit_http(page_size=2)
    assert response.status_code == 200
    body = response.json()
    assert body["outcome"] == "stale"
    assert body["error"]["reason"] == "Expired"


def test_openapi_documents_audit_endpoint():
    schema = client.get("/openapi.json").json()
    assert "/audit/{namespace}/{name}" in schema["paths"]
    operation = schema["paths"]["/audit/{namespace}/{name}"]["post"]
    assert "409" in operation["responses"]
    param_names = {p["name"] for p in operation["parameters"]}
    assert {"namespace", "name", "page_size"} <= param_names
