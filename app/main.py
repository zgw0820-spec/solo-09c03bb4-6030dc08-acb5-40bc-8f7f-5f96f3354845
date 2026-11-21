"""BackupPolicy webhook + 盘点 API 的 FastAPI HTTP 层。

* ``/convert``：ConversionReview 协议解包/打包，规则在 app.conversion；
* ``/audit/{namespace}/{name}``：发起一轮快照盘点，规则在 app.audit。

本层只做参数校验、依赖注入（Kubernetes 客户端）与错误到 HTTP 的映射。
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager

from fastapi import Body, FastAPI, Path, Query
from fastapi.responses import JSONResponse

from .audit import (
    AuditRejected,
    BackupPolicyNotFound,
    InvalidBackupPolicy,
    audit_policy,
)
from .conversion import (
    GROUP,
    SUPPORTED_VERSIONS,
    BatchConversionError,
    convert_objects,
)
from .examples import OPENAPI_EXAMPLES
from .kube import KubernetesClient, KubernetesError

logger = logging.getLogger("backup-policy-conversion")

# 本 webhook 说 ConversionReview v1；对旧版 v1beta1 形状的请求也宽容回显。
_REVIEW_API_VERSIONS = {"apiextensions.k8s.io/v1", "apiextensions.k8s.io/v1beta1"}
_DEFAULT_REVIEW_API_VERSION = "apiextensions.k8s.io/v1"

#: 本地 Compose 里指向 mockkube 服务；真实集群里指向 API server。
KUBERNETES_API_URL = os.environ.get("KUBERNETES_API_URL", "http://127.0.0.1:8081")
KUBERNETES_TOKEN = os.environ.get("KUBERNETES_TOKEN") or None
#: 未显式给 token 时尝试 In-cluster ServiceAccount 的 token 文件。
KUBERNETES_TOKEN_FILE = os.environ.get("KUBERNETES_TOKEN_FILE") or (
    None if KUBERNETES_TOKEN else "/var/run/secrets/kubernetes.io/serviceaccount/token"
)
#: HTTPS 集群（In-cluster）默认信任挂载的集群 CA。
KUBERNETES_CA_FILE = os.environ.get("KUBERNETES_CA_FILE") or (
    None if KUBERNETES_API_URL.startswith("http://")
    else "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

app = FastAPI(
    title="BackupPolicy Webhook & Snapshot Audit",
    version="1.1.0",
    description="""
Kubernetes CRD `backuppolicies.backup.example.com` 的版本转换 webhook：
**v1beta1 是存储版本**，v1alpha1 是值班员旧工具使用的旧版。

## 在 Swagger 里演示"旧客户端读 → 改 → 写回"

打开 `POST /convert`，在请求体的 **Example** 下拉框里按编号选择：

1. **① 旧客户端读取**：存储的 v1beta1 → v1alpha1，per-target 设置收进兼容注解
   （`cache` 的 3 恰好等于默认值，也照样记录）；
2. **② 写回：改默认值 + 重排目标**：默认值 3→10、目标重排，按名字恢复单独设置，
   `cache` 仍是显式 3（不会跟随新默认 10）；
3. **③ 删除后同名重加**：`cache` 被删过一轮再加回 → 沿用当前默认，不带单独份数；
4. **④ 丢注解 / ⑤ 注解损坏**：带 resourceVersion 写回 → 整批 Failure 并指出问题对象；
5. **⑥ 旧客户端新建**：无 resourceVersion，免注解，正常转换。

webhook 无状态：执行①后 `docker compose restart webhook` 再执行②，结果不变。

## 盘点 API

`POST /audit/{namespace}/{name}`：分页读取 VolumeSnapshot，统计每个目标的可用份数
与保留缺口，收齐同一份清单后写入 BackupPolicy 的 `/status`。Compose 默认连本地
Kubernetes 模拟端（`mockkube`，端口 8081），打开其 `/docs` 可制造续页过期、
盘点中改策略 / 同名重建 / 并发写回等场景。
""",
)


def _existing(path: str | None) -> str | None:
    return path if path and os.path.exists(path) else None


@contextmanager
def _kube_client() -> Iterator[KubernetesClient]:
    client = KubernetesClient(
        KUBERNETES_API_URL,
        token=KUBERNETES_TOKEN,
        token_file=_existing(KUBERNETES_TOKEN_FILE),
        ca_file=_existing(KUBERNETES_CA_FILE),
    )
    try:
        yield client
    finally:
        client.close()


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.post("/convert")
def convert(review: dict = Body(..., openapi_examples=OPENAPI_EXAMPLES)) -> JSONResponse:
    review_api_version = review.get("apiVersion")
    if review_api_version not in _REVIEW_API_VERSIONS:
        review_api_version = _DEFAULT_REVIEW_API_VERSION

    request = review.get("request")
    if not isinstance(request, dict):
        return _failure(None, review_api_version, 'malformed ConversionReview: missing "request" object')
    uid = request.get("uid")

    desired = request.get("desiredAPIVersion")
    prefix = f"{GROUP}/"
    if (
        not isinstance(desired, str)
        or not desired.startswith(prefix)
        or desired[len(prefix):] not in SUPPORTED_VERSIONS
    ):
        expected = ", ".join(f"{GROUP}/{v}" for v in SUPPORTED_VERSIONS)
        return _failure(uid, review_api_version, f"unsupported desiredAPIVersion {desired!r}; expected one of: {expected}")
    desired_version = desired[len(prefix):]

    objects = request.get("objects")
    if objects is None:
        objects = []
    if not isinstance(objects, list):
        return _failure(uid, review_api_version, 'malformed ConversionReview: "request.objects" must be a list')

    try:
        converted = convert_objects(objects, desired_version)
    except BatchConversionError as exc:
        # 整批失败：HTTP 200 + result.status=Failure，convertedObjects 缺省。
        logger.warning("conversion rejected: %s", exc)
        return _failure(uid, review_api_version, str(exc))

    return JSONResponse(
        {
            "apiVersion": review_api_version,
            "kind": "ConversionReview",
            "response": {
                "uid": uid,
                "result": {"status": "Success"},
                "convertedObjects": converted,
            },
        }
    )


def _failure(uid: object, review_api_version: str, message: str) -> JSONResponse:
    response: dict = {"result": {"status": "Failure", "message": message}}
    if uid is not None:
        response["uid"] = uid
    return JSONResponse(
        {
            "apiVersion": review_api_version,
            "kind": "ConversionReview",
            "response": response,
        }
    )


# ---------------------------------------------------------------------------
# 盘点 API
# ---------------------------------------------------------------------------

@app.post(
    "/audit/{namespace}/{name}",
    tags=["audit"],
    summary="盘点 BackupPolicy 目标的快照保留缺口并写入 /status",
    response_description="本轮盘点结果（written 或 stale）",
    responses={
        404: {"description": "BackupPolicy 不存在"},
        409: {"description": "拒绝写回：策略被删除/同名重建，或另一轮盘点已更新结果"},
        422: {"description": "BackupPolicy spec/status 形状不合法"},
        502: {"description": "读取 Kubernetes API 或写回 status 失败"},
    },
)
def audit_backup_policy(
    namespace: str = Path(..., description="BackupPolicy 所在命名空间"),
    name: str = Path(..., description="BackupPolicy 名称"),
    page_size: int = Query(50, ge=1, le=500, description="VolumeSnapshot 分页大小"),
):
    try:
        with _kube_client() as client:
            result = audit_policy(client, namespace, name, page_size=page_size)
    except BackupPolicyNotFound as exc:
        return _audit_error(404, "BackupPolicyNotFound", str(exc))
    except AuditRejected as exc:
        return _audit_error(409, exc.reason, exc.message)
    except InvalidBackupPolicy as exc:
        return _audit_error(422, "InvalidBackupPolicy", exc.message)
    except KubernetesError as exc:
        return _audit_error(502, exc.reason or "KubernetesAPIError", exc.message)
    return result


def _audit_error(status_code: int, reason: str, message: str) -> JSONResponse:
    logger.warning("audit rejected/failed (%s): %s", reason, message)
    return JSONResponse(
        {
            "error": {
                "code": status_code,
                "reason": reason,
                "message": message,
            }
        },
        status_code=status_code,
    )
