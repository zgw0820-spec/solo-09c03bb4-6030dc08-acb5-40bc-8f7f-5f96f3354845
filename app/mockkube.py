"""本地 Kubernetes API 模拟端（仅用于 Compose / Swagger / 测试）。

模拟的范围刚好够盘点流程使用：

* ``GET /apis/backup.example.com/{v1beta1|v1alpha1}/namespaces/{ns}/backuppolicies/{name}``
  —— v1alpha1 视图借助真实转换逻辑生成，用来验证 status 在两个版本都能读到；
* ``PUT .../backuppolicies/{name}/status`` —— 校验 UID 与 resourceVersion，
  模拟 Kubernetes 的乐观并发（409 Conflict）；
* ``GET /apis/snapshot.storage.k8s.io/v1/namespaces/{ns}/volumesnapshots``
  —— ``limit`` / ``continue`` 分页，每页 resourceVersion 与首屏一致；
  continue token 可被显式作废，之后返回 410 Gone。

控制面（``/_mock/...``）用于在演示/测试中制造并发场景：
改 spec（generation+1）、同名重建（换 UID）、另一轮盘点更新 status.audit、
状态写回冲突、以及"服务完第 N 个列表请求后"触发上述动作。
"""

from __future__ import annotations

import copy
import threading
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from fastapi import Body, FastAPI, Query
from fastapi.responses import JSONResponse

from .conversion import convert_object

SNAPSHOT_API_VERSION = "snapshot.storage.k8s.io/v1"
POLICY_GROUP = "backup.example.com"
TARGET_ANNOTATION = "backup.example.com/target"

DEMO_NAMESPACE = "default"
DEMO_POLICY = "web-backup"
DEMO_POLICY_UID = "3f8c2a70-0000-4000-8000-00000000a001"


# ---------------------------------------------------------------------------
# 状态
# ---------------------------------------------------------------------------

@dataclass
class MockState:
    resource_version: int = 5000
    policies: dict[tuple[str, str], dict] = field(default_factory=dict)
    snapshots: dict[str, list[dict]] = field(default_factory=dict)
    tokens: dict[str, dict] = field(default_factory=dict)
    fail_continue_pages: int = 0
    status_conflict: bool = False
    list_requests: int = 0
    status_put_attempts: int = 0
    status_puts: int = 0
    errors: list[dict] = field(default_factory=list)
    scheduled: list[dict] = field(default_factory=list)
    token_seq: int = 0

    def next_rv(self) -> str:
        self.resource_version += 1
        return str(self.resource_version)


STATE = MockState()
LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# 演示数据
# ---------------------------------------------------------------------------

def _snapshot(
    name: str,
    target: str | None,
    *,
    owner_uid: str = DEMO_POLICY_UID,
    ready: bool = True,
    deleting: bool = False,
    namespace: str = DEMO_NAMESPACE,
) -> dict:
    annotations: dict[str, str] = {}
    if target is not None:
        annotations[TARGET_ANNOTATION] = target
    meta: dict = {
        "name": name,
        "namespace": namespace,
        "uid": str(uuid.uuid5(uuid.NAMESPACE_URL, f"snapshot:{namespace}/{name}")),
        "resourceVersion": "4100",
        "creationTimestamp": "2026-11-20T10:00:00Z",
        "annotations": annotations,
        "ownerReferences": [
            {
                "apiVersion": f"{POLICY_GROUP}/v1beta1",
                "kind": "BackupPolicy",
                "name": DEMO_POLICY,
                "uid": owner_uid,
                "controller": True,
                "blockOwnerDeletion": True,
            }
        ],
    }
    if deleting:
        meta["deletionTimestamp"] = "2026-11-21T08:00:00Z"
        meta["finalizers"] = ["snapshot.storage.kubernetes.io/volumesnapshot-protection"]
    return {
        "apiVersion": SNAPSHOT_API_VERSION,
        "kind": "VolumeSnapshot",
        "metadata": meta,
        "spec": {"source": {"persistentVolumeClaimName": f"pvc-{name}"}},
        "status": {"readyToUse": ready},
    }


def demo_policy() -> dict:
    return {
        "apiVersion": f"{POLICY_GROUP}/v1beta1",
        "kind": "BackupPolicy",
        "metadata": {
            "name": DEMO_POLICY,
            "namespace": DEMO_NAMESPACE,
            "uid": DEMO_POLICY_UID,
            "resourceVersion": "4950",
            "generation": 7,
            "creationTimestamp": "2026-11-01T00:00:00Z",
            "labels": {"team": "payments"},
        },
        "spec": {
            "defaultRetention": 3,
            "targets": [
                {"name": "db", "retention": 5},
                {"name": "cache", "retention": 3},
                {"name": "logs"},
                {"name": "metrics", "retention": 2},
            ],
        },
        "status": {
            "observedGeneration": 7,
            "message": "controller heartbeat ok",
            "conditions": [
                {"type": "Ready", "status": "True", "reason": "Reconciled",
                 "lastTransitionTime": "2026-11-20T09:00:00Z"}
            ],
        },
    }


def demo_snapshots() -> dict[str, list[dict]]:
    return {
        DEMO_NAMESPACE: [
            # db: 2 份可用（保留 5 -> 缺 3）
            _snapshot("db-1", "db"),
            _snapshot("db-2", "db"),
            _snapshot("db-other-owner", "db", owner_uid="someone-else"),
            _snapshot("db-not-ready", "db", ready=False),
            _snapshot("db-terminating", "db", deleting=True),
            # cache: 3 份可用（保留 3 -> 缺 0）
            _snapshot("cache-1", "cache"),
            _snapshot("cache-2", "cache"),
            _snapshot("cache-3", "cache"),
            # logs: 1 份可用（默认保留 3 -> 缺 2）
            _snapshot("logs-1", "logs"),
            # metrics: 0 份（保留 2 -> 缺 2）
            # 不属于策略目标的注解被忽略
            _snapshot("ghost-1", "ghost"),
            # 缺 target 注解，忽略
            _snapshot("untagged-1", None),
        ]
    }


def reset_state(
    policies: list[dict] | None = None,
    snapshots: dict[str, list[dict]] | None = None,
) -> None:
    global STATE
    with LOCK:
        STATE = MockState(
            policies={
                (p["metadata"]["namespace"], p["metadata"]["name"]): copy.deepcopy(p)
                for p in (policies if policies is not None else [demo_policy()])
            },
            snapshots=copy.deepcopy(snapshots if snapshots is not None else demo_snapshots()),
        )


@asynccontextmanager
async def _lifespan(_: FastAPI):
    reset_state()
    yield


app = FastAPI(
    title="Local Kubernetes API Mock",
    version="1.0.0",
    description="VolumeSnapshot 分页 / BackupPolicy status 并发语义的本地模拟端。",
    lifespan=_lifespan,
)


# ---------------------------------------------------------------------------
# BackupPolicy
# ---------------------------------------------------------------------------

def _policy_key(namespace: str, name: str) -> tuple[str, str]:
    return (namespace, name)


def _not_found(kind: str, namespace: str, name: str) -> JSONResponse:
    return _status(
        404,
        "NotFound",
        f'{kind} "{namespace}/{name}" not found',
    )


def _status(code: int, reason: str, message: str) -> JSONResponse:
    return JSONResponse(
        {
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "message": message,
            "reason": reason,
            "code": code,
        },
        status_code=code,
    )


@app.get(
    "/apis/backup.example.com/{version}/namespaces/{namespace}/backuppolicies/{name}",
    tags=["mock-kube"],
    summary="读取 BackupPolicy（v1beta1 存储；v1alpha1 走真实转换逻辑）",
)
def get_policy(version: str, namespace: str, name: str):
    with LOCK:
        stored = STATE.policies.get(_policy_key(namespace, name))
        if stored is None:
            return _not_found("BackupPolicy", namespace, name)
        obj = copy.deepcopy(stored)
    if version == "v1beta1":
        return obj
    if version == "v1alpha1":
        return convert_object(obj, "v1alpha1")
    return _status(404, "NotFound", f"unsupported version {version!r}")


@app.put(
    "/apis/backup.example.com/v1beta1/namespaces/{namespace}/backuppolicies/{name}/status",
    tags=["mock-kube"],
    summary="写入 status 子资源（校验 UID / resourceVersion）",
)
def put_policy_status(namespace: str, name: str, body: dict = Body(...)):
    with LOCK:
        STATE.status_put_attempts += 1
        if STATE.status_conflict:
            STATE.status_conflict = False
            return _status(
                409,
                "Conflict",
                f'Operation cannot be fulfilled on backuppolicies.backup.example.com "{name}": '
                "the object has been modified; please apply your changes to the latest version "
                "and try again",
            )

        stored = STATE.policies.get(_policy_key(namespace, name))
        if stored is None:
            return _not_found("BackupPolicy", namespace, name)

        body_meta = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
        if body_meta.get("uid") != stored["metadata"]["uid"]:
            return _status(
                409,
                "Conflict",
                f'Operation cannot be fulfilled on backuppolicies.backup.example.com "{name}": '
                "the object UID does not match (the policy may have been recreated)",
            )
        if body_meta.get("resourceVersion") != stored["metadata"]["resourceVersion"]:
            return _status(
                409,
                "Conflict",
                f'Operation cannot be fulfilled on backuppolicies.backup.example.com "{name}": '
                f"the object has been modified (resourceVersion "
                f"{stored['metadata']['resourceVersion']!r} != "
                f"{body_meta.get('resourceVersion')!r})",
            )

        stored["status"] = copy.deepcopy(body.get("status") or {})
        stored["metadata"]["resourceVersion"] = STATE.next_rv()
        STATE.status_puts += 1
        return copy.deepcopy(stored)


# ---------------------------------------------------------------------------
# VolumeSnapshot 分页
# ---------------------------------------------------------------------------

def _expire_all_tokens() -> None:
    for token in STATE.tokens.values():
        token["expired"] = True


def _run_scheduled_actions() -> None:
    """执行到期的调度动作。

    ``after_list_request`` 是相对量：从调度那一刻起，再服务完这么多页后触发。
    这样测试/演示无需知道当前累计了多少请求。
    """
    for action in STATE.scheduled:
        if action["fired"]:
            continue
        scheduled_at = action.get("scheduled_at", 0)
        if STATE.list_requests - scheduled_at >= action["after_list_request"]:
            action["fired"] = True
            _apply_action(action["action"], action.get("params") or {})


def _apply_action(action: str, params: dict) -> None:
    namespace = params.get("namespace", DEMO_NAMESPACE)
    name = params.get("name", DEMO_POLICY)
    stored = STATE.policies.get((namespace, name))

    if action == "expire_continue_tokens":
        _expire_all_tokens()

    elif action == "fail_continue_page":
        STATE.fail_continue_pages += 1

    elif action == "inconsistent_pages":
        # 让已发出的续页 token 声称来自另一份清单（RV 不同），
        # 模拟分页期间集合被整体替换。
        for token in STATE.tokens.values():
            token["rv"] = str(int(token["rv"]) - 1)

    elif action == "bump_generation" and stored is not None:
        spec = params.get("spec")
        if isinstance(spec, dict):
            stored["spec"] = copy.deepcopy(spec)
        stored["metadata"]["generation"] = int(stored["metadata"].get("generation", 1)) + 1
        stored["metadata"]["resourceVersion"] = STATE.next_rv()

    elif action == "recreate_policy" and stored is not None:
        stored["metadata"]["uid"] = str(uuid.uuid4())
        stored["metadata"]["generation"] = 1
        stored["metadata"]["resourceVersion"] = STATE.next_rv()
        stored["status"] = {}

    elif action == "delete_policy":
        STATE.policies.pop((namespace, name), None)

    elif action == "set_audit_run_id" and stored is not None:
        status = stored.setdefault("status", {})
        audit = status.setdefault("audit", {})
        audit["runID"] = params.get("runID", str(uuid.uuid4()))
        stored["metadata"]["resourceVersion"] = STATE.next_rv()

    elif action == "status_conflict":
        STATE.status_conflict = True

    else:
        STATE.errors.append({"action": action, "message": "unknown or inapplicable action"})


@app.get(
    "/apis/snapshot.storage.k8s.io/v1/namespaces/{namespace}/volumesnapshots",
    tags=["mock-kube"],
    summary="分页列出 VolumeSnapshot（limit / continue）",
)
def list_snapshots(
    namespace: str,
    limit: int = Query(50, ge=1, le=500),
    continue_token: str | None = Query(None, alias="continue"),
):
    with LOCK:
        if continue_token:
            if STATE.fail_continue_pages > 0:
                STATE.fail_continue_pages -= 1
                return _status(
                    500,
                    "InternalError",
                    "failed to read the requested page of VolumeSnapshots: "
                    "etcdserver: request timed out",
                )
            token = STATE.tokens.get(continue_token)
            if token is None or token["expired"]:
                return _status(
                    410,
                    "Expired",
                    f"continue token {continue_token!r} is no longer valid: "
                    "the requested resourceVersion is too old",
                )
            items = token["items"]
            offset = token["offset"]
            list_rv = token["rv"]
        else:
            items = copy.deepcopy(STATE.snapshots.get(namespace, []))
            offset = 0
            list_rv = str(STATE.resource_version)

        page = items[offset:offset + limit]
        next_offset = offset + len(page)
        meta: dict = {
            "resourceVersion": list_rv,
            "remainingItemCount": max(len(items) - next_offset, 0),
        }
        if next_offset < len(items):
            STATE.token_seq += 1
            next_token = f"mock-token-{STATE.token_seq}"
            STATE.tokens[next_token] = {
                "namespace": namespace,
                "items": items,
                "offset": next_offset,
                "rv": list_rv,
                "expired": False,
            }
            meta["continue"] = next_token

        STATE.list_requests += 1
        _run_scheduled_actions()
        return {
            "apiVersion": SNAPSHOT_API_VERSION,
            "kind": "VolumeSnapshotList",
            "metadata": meta,
            "items": page,
        }


# ---------------------------------------------------------------------------
# 控制面
# ---------------------------------------------------------------------------

@app.post("/_mock/reset", tags=["mock-control"], summary="重置为演示数据或自定义数据")
def reset(body: dict = Body(default={})):
    reset_state(body.get("policies"), body.get("snapshots"))
    return {"status": "reset"}


@app.get("/_mock/state", tags=["mock-control"], summary="查看模拟端当前状态与请求计数")
def state():
    with LOCK:
        return {
            "resourceVersion": str(STATE.resource_version),
            "listRequests": STATE.list_requests,
            "statusPutAttempts": STATE.status_put_attempts,
            "failContinuePages": STATE.fail_continue_pages,
            "statusPuts": STATE.status_puts,
            "continueTokens": {
                token: {"expired": entry["expired"], "offset": entry["offset"]}
                for token, entry in STATE.tokens.items()
            },
            "policies": [
                copy.deepcopy(policy) for policy in STATE.policies.values()
            ],
            "snapshotCounts": {
                namespace: len(items) for namespace, items in STATE.snapshots.items()
            },
            "scheduledActions": copy.deepcopy(STATE.scheduled),
            "errors": copy.deepcopy(STATE.errors),
        }


@app.post("/_mock/policies/{namespace}/{name}/spec", tags=["mock-control"],
          summary="修改策略 spec（generation+1，模拟盘点中编辑策略）")
def replace_spec(namespace: str, name: str, spec: dict = Body(...)):
    with LOCK:
        stored = STATE.policies.get((namespace, name))
        if stored is None:
            return _not_found("BackupPolicy", namespace, name)
        stored["spec"] = copy.deepcopy(spec)
        stored["metadata"]["generation"] = int(stored["metadata"].get("generation", 1)) + 1
        stored["metadata"]["resourceVersion"] = STATE.next_rv()
        return copy.deepcopy(stored)


@app.post("/_mock/policies/{namespace}/{name}/recreate", tags=["mock-control"],
          summary="同名重建策略（换 UID、清空 status）")
def recreate(namespace: str, name: str):
    with LOCK:
        stored = STATE.policies.get((namespace, name))
        if stored is None:
            return _not_found("BackupPolicy", namespace, name)
        stored["metadata"]["uid"] = str(uuid.uuid4())
        stored["metadata"]["generation"] = 1
        stored["metadata"]["resourceVersion"] = STATE.next_rv()
        stored["status"] = {}
        return copy.deepcopy(stored)


@app.post("/_mock/policies/{namespace}/{name}/audit-run-id", tags=["mock-control"],
          summary="模拟另一轮盘点更新 status.audit.runID")
def set_audit_run_id(namespace: str, name: str, body: dict = Body(default={})):
    with LOCK:
        stored = STATE.policies.get((namespace, name))
        if stored is None:
            return _not_found("BackupPolicy", namespace, name)
        status = stored.setdefault("status", {})
        audit = status.setdefault("audit", {})
        audit["runID"] = body.get("runID", str(uuid.uuid4()))
        stored["metadata"]["resourceVersion"] = STATE.next_rv()
        return copy.deepcopy(stored)


@app.post("/_mock/expire-continue-tokens", tags=["mock-control"],
          summary="作废全部 continue token（下一次续页返回 410）")
def expire_continue_tokens():
    with LOCK:
        _expire_all_tokens()
        return {"status": "expired", "tokens": len(STATE.tokens)}


@app.post("/_mock/fail-continue-pages", tags=["mock-control"],
          summary="让接下来的 N 个续页请求返回 500（模拟读取失败）")
def fail_continue_pages(body: dict = Body(default={"count": 1})):
    count = body.get("count", 1)
    if not isinstance(count, int) or count < 1:
        return JSONResponse({"error": "count must be a positive integer"}, status_code=400)
    with LOCK:
        STATE.fail_continue_pages += count
        return {"failContinuePages": STATE.fail_continue_pages}


@app.post("/_mock/status-conflict", tags=["mock-control"],
          summary="让下一次 status 写回返回 409 Conflict")
def inject_status_conflict(body: dict = Body(default={"enabled": True})):
    with LOCK:
        STATE.status_conflict = bool(body.get("enabled", True))
        return {"statusConflict": STATE.status_conflict}


@app.post("/_mock/schedule", tags=["mock-control"],
          summary="在服务完第 N 个列表请求后触发动作（制造盘点中的并发）")
def schedule(body: dict = Body(...)):
    action = body.get("action")
    allowed = {
        "bump_generation",
        "recreate_policy",
        "set_audit_run_id",
        "expire_continue_tokens",
        "fail_continue_page",
        "delete_policy",
        "inconsistent_pages",
        "status_conflict",
    }
    if action not in allowed:
        return JSONResponse({"error": f"action must be one of {sorted(allowed)}"}, status_code=400)
    after = body.get("afterListRequest")
    if not isinstance(after, int) or after < 0:
        return JSONResponse({"error": "afterListRequest must be a non-negative integer"}, status_code=400)
    with LOCK:
        entry = {
            "after_list_request": after,
            "scheduled_at": STATE.list_requests,
            "action": action,
            "params": body.get("params") or {},
            "fired": False,
        }
        STATE.scheduled.append(entry)
        return entry


@app.post("/_mock/clear-schedule", tags=["mock-control"],
          summary="清空未触发的调度动作")
def clear_schedule():
    with LOCK:
        STATE.scheduled = []
        return {"status": "cleared"}
