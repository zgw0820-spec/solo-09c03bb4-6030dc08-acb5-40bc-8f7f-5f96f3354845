"""BackupPolicy 快照盘点（stocktake）逻辑。

控制器需要知道"每个目标还缺几份快照"，本模块负责：

1. 读取 BackupPolicy（v1beta1 存储版本），记下 UID / generation / 当前盘点 runID；
2. **分页**读取同命名空间的 VolumeSnapshot，只有整份清单收齐（每页 resourceVersion
   一致且无续页错误）才允许写 ``/status``；
3. 只统计满足全部条件的快照：
   * ``ownerReferences`` 中存在 UID 等于 BackupPolicy UID 的属主；
   * ``status.readyToUse`` 严格为 ``true``；
   * 没有 ``metadata.deletionTimestamp``（未进入删除）；
   * 带 ``backup.example.com/target`` 注解，用注解值识别目标。
4. 用现行策略计算每个目标的 ``retention``（单独设置优先，否则 defaultRetention）、
   ``available``（可用份数）与 ``gap``（还缺几份，``max(retention - available, 0)``）。

一致性 / 并发语义
------------------
* 盘点期间策略被修改：**不重扫快照**，用已经收齐的同一份清单按最新策略重算；
* 盘点期间策略被删除或同名重建（UID 变化）：拒绝写回；
* 另一轮盘点已更新 ``status.audit``（runID 变化）或写 ``/status`` 时发生
  resourceVersion 冲突：拒绝写回；
* 续页过期（HTTP 410）或任何读取失败：保留上次 ``status.audit`` 不变，只写入
  ``status.auditError`` 说明原因，并返回 stale 结果。

``status`` 中只增删本模块拥有的 ``audit`` / ``auditError``，其余字段（
observedGeneration、message、conditions 等）原样保留。
"""

from __future__ import annotations

import copy
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from .kube import (
    Conflict,
    KubernetesClient,
    KubernetesError,
    NotFound,
)

#: 识别快照所属备份目标的注解。
TARGET_ANNOTATION = "backup.example.com/target"

_STATUS_AUDIT = "audit"
_STATUS_AUDIT_ERROR = "auditError"


class BackupPolicyNotFound(Exception):
    """按 namespace/name 找不到 BackupPolicy。"""


class AuditRejected(Exception):
    """拒绝本轮写回（策略重建 / 并发盘点 / 写回冲突）。"""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason
        self.message = message


class InvalidBackupPolicy(Exception):
    """BackupPolicy 的 spec/status 形状不合法，无法计算保留缺口。"""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


@dataclass(frozen=True)
class _Baseline:
    uid: str
    generation: int
    audit_run_id: object  # str | None


def audit_policy(
    client: KubernetesClient,
    namespace: str,
    name: str,
    *,
    page_size: int = 50,
    now: datetime | None = None,
) -> dict:
    """执行一轮盘点并写入 ``/status``，返回面向 API 的结构化结果。

    可能抛出的异常：BackupPolicyNotFound、AuditRejected、InvalidBackupPolicy、
    KubernetesError（读初始策略失败 / 写状态失败）。
    """
    try:
        initial = client.get_policy(namespace, name)
    except NotFound as exc:
        raise BackupPolicyNotFound(f'BackupPolicy "{namespace}/{name}" not found') from exc

    baseline = _baseline_of(initial, namespace, name)

    try:
        snapshots, list_rv = _collect_snapshot_pages(client, namespace, page_size)
    except KubernetesError as exc:
        return _handle_scan_failure(
            client,
            namespace,
            name,
            baseline,
            exc,
            now=now,
        )

    latest = _current_policy(client, namespace, name, baseline)
    recomputed = _generation_of(latest) != baseline.generation
    targets = _targets_with_counts(latest, _count_available(snapshots, baseline.uid))
    audit_record = {
        "runID": str(uuid.uuid4()),
        "policyUID": baseline.uid,
        "generation": _generation_of(latest),
        "snapshotResourceVersion": list_rv,
        "completedAt": _timestamp(now),
        "targets": targets,
    }

    updated = _write_status(
        client,
        latest,
        namespace=namespace,
        name=name,
        audit_record=audit_record,
        audit_error=None,
    )
    return {
        "outcome": "written",
        "namespace": namespace,
        "name": name,
        "policyUID": baseline.uid,
        "policyGeneration": _generation_of(latest),
        "snapshotResourceVersion": list_rv,
        "recomputed": recomputed,
        "audit": audit_record,
        "error": None,
        "statusResourceVersion": _meta(updated).get("resourceVersion"),
    }


# ---------------------------------------------------------------------------
# 快照清单
# ---------------------------------------------------------------------------

def _collect_snapshot_pages(
    client: KubernetesClient, namespace: str, page_size: int
) -> tuple[list[dict], str]:
    """分页收齐快照；返回 (items, 整份清单的 resourceVersion)。

    只有所有页的 resourceVersion 一致才算"同一份清单"；续页 token 重复出现视为
    异常，避免死循环。
    """
    if page_size < 1:
        raise ValueError("page_size must be >= 1")

    items: list[dict] = []
    seen_tokens: set[str] = set()
    list_rv: str | None = None
    continue_token: str | None = None

    while True:
        page = client.list_volumesnapshots(namespace, page_size, continue_token)
        meta = page.get("metadata")
        meta = meta if isinstance(meta, dict) else {}
        page_rv = meta.get("resourceVersion")
        if not isinstance(page_rv, str) or not page_rv:
            raise KubernetesError(
                "InvalidKubernetesResponse",
                "VolumeSnapshot list page is missing metadata.resourceVersion",
            )
        if list_rv is None:
            list_rv = page_rv
        elif page_rv != list_rv:
            raise KubernetesError(
                "InconsistentListVersion",
                "VolumeSnapshot list pages have different resourceVersion "
                f"({list_rv!r} vs {page_rv!r}); refusing to treat them as one inventory",
            )

        page_items = page.get("items", [])
        if not isinstance(page_items, list):
            raise KubernetesError(
                "InvalidKubernetesResponse",
                "VolumeSnapshot list page has non-list metadata.items",
            )
        items.extend(item for item in page_items if isinstance(item, dict))

        next_token = meta.get("continue")
        if not next_token:
            break
        if not isinstance(next_token, str):
            raise KubernetesError(
                "InvalidKubernetesResponse",
                "VolumeSnapshot list page has non-string metadata.continue",
            )
        if next_token in seen_tokens:
            raise KubernetesError(
                "InvalidKubernetesResponse",
                f"VolumeSnapshot list repeated continue token {next_token!r}",
            )
        seen_tokens.add(next_token)
        continue_token = next_token

    if list_rv is None:  # pragma: no cover - 首屏缺 RV 已在上方报错
        raise KubernetesError(
            "InvalidKubernetesResponse",
            "VolumeSnapshot list never returned a resourceVersion",
        )
    return items, list_rv


def _count_available(snapshots: list[dict], policy_uid: str) -> dict[str, int]:
    """按 target 注解统计可用快照数（只计属主 UID 匹配、ready、未删除的）。"""
    counts: dict[str, int] = {}
    for snapshot in snapshots:
        meta = snapshot.get("metadata")
        meta = meta if isinstance(meta, dict) else {}
        if meta.get("deletionTimestamp") is not None:
            continue

        status = snapshot.get("status")
        status = status if isinstance(status, dict) else {}
        if status.get("readyToUse") is not True:
            continue

        owners = meta.get("ownerReferences")
        if not isinstance(owners, list):
            continue
        if not any(
            isinstance(owner, dict) and owner.get("uid") == policy_uid
            for owner in owners
        ):
            continue

        annotations = meta.get("annotations")
        annotations = annotations if isinstance(annotations, dict) else {}
        target = annotations.get(TARGET_ANNOTATION)
        if not isinstance(target, str) or not target:
            continue
        counts[target] = counts.get(target, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# 策略 / 目标缺口
# ---------------------------------------------------------------------------

def _targets_with_counts(policy: dict, counts: dict[str, int]) -> list[dict]:
    spec = policy.get("spec")
    if spec is None:
        spec = {}
    if not isinstance(spec, dict):
        raise InvalidBackupPolicy("BackupPolicy spec must be an object")

    default_retention = _retention(spec.get("defaultRetention"), "spec.defaultRetention")
    if default_retention is None:
        # CRD schema 给了 default: 3，存储对象缺这个字段说明数据不正常；
        # 静默按 0 处理会把所有"跟随默认"的目标算成不缺口，掩盖问题。
        raise InvalidBackupPolicy("spec.defaultRetention is required")

    targets = spec.get("targets")
    if targets is None:
        targets = []
    if not isinstance(targets, list):
        raise InvalidBackupPolicy("BackupPolicy spec.targets must be a list")

    result: list[dict] = []
    seen: set[str] = set()
    for index, entry in enumerate(targets):
        if not isinstance(entry, dict):
            raise InvalidBackupPolicy(f"spec.targets[{index}] must be an object")
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            raise InvalidBackupPolicy(
                f"spec.targets[{index}].name must be a non-empty string"
            )
        if name in seen:
            raise InvalidBackupPolicy(f"duplicate target name {name!r} in spec.targets")
        seen.add(name)

        retention = _retention(
            entry.get("retention"), f"spec.targets[{index}].retention"
        )
        if retention is None:
            retention = default_retention
        available = counts.get(name, 0)
        result.append(
            {
                "name": name,
                "retention": retention,
                "available": available,
                "gap": max(retention - available, 0),
            }
        )
    return result


def _retention(value: object, where: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidBackupPolicy(f"{where} must be a non-negative integer, got {value!r}")
    return value


# ---------------------------------------------------------------------------
# 失败路径：保留上次 audit，只写 auditError
# ---------------------------------------------------------------------------

def _handle_scan_failure(
    client: KubernetesClient,
    namespace: str,
    name: str,
    baseline: _Baseline,
    exc: KubernetesError,
    *,
    now: datetime | None,
) -> dict:
    latest = _current_policy(client, namespace, name, baseline)
    prior_status = _status_of(latest)
    prior_audit = prior_status.get(_STATUS_AUDIT)
    error_record = {
        "reason": exc.reason or "KubernetesAPIError",
        "message": exc.message or str(exc),
        "at": _timestamp(now),
    }

    updated = _write_status(
        client,
        latest,
        namespace=namespace,
        name=name,
        audit_record=None,  # 保持上次结果，不替换
        audit_error=error_record,
    )
    return {
        "outcome": "stale",
        "namespace": namespace,
        "name": name,
        "policyUID": baseline.uid,
        "policyGeneration": _generation_of(latest),
        "snapshotResourceVersion": None,
        "recomputed": False,
        "audit": prior_audit if isinstance(prior_audit, dict) else None,
        "error": error_record,
        "statusResourceVersion": _meta(updated).get("resourceVersion"),
    }


# ---------------------------------------------------------------------------
# 读最新策略 / 并发校验 / 写状态
# ---------------------------------------------------------------------------

def _current_policy(
    client: KubernetesClient,
    namespace: str,
    name: str,
    baseline: _Baseline,
) -> dict:
    """重新读取策略，并校验对象身份与盘点结果未被另一轮改写。"""
    try:
        latest = client.get_policy(namespace, name)
    except NotFound as exc:
        raise AuditRejected(
            "PolicyDeleted",
            f'BackupPolicy "{namespace}/{name}" was deleted during the audit; '
            "refusing to write status",
        ) from exc

    latest_uid = _meta(latest).get("uid")
    if latest_uid != baseline.uid:
        raise AuditRejected(
            "PolicyRecreated",
            "BackupPolicy was deleted and recreated with the same name during the "
            f"audit (UID {baseline.uid!r} -> {latest_uid!r}); refusing to write status",
        )

    latest_run_id = _audit_run_id(latest)
    if latest_run_id != baseline.audit_run_id:
        raise AuditRejected(
            "ConcurrentAuditUpdate",
            "another reconciliation updated status.audit during this audit "
            f"(runID {baseline.audit_run_id!r} -> {latest_run_id!r}); "
            "refusing to overwrite its result",
        )
    return latest


def _write_status(
    client: KubernetesClient,
    policy: dict,
    *,
    namespace: str,
    name: str,
    audit_record: dict | None,
    audit_error: dict | None,
) -> dict:
    """保留其他 status 字段，写回本模块拥有的 audit/auditError。"""
    body = copy.deepcopy(policy)
    status = _status_of(body)
    if audit_record is not None:
        status[_STATUS_AUDIT] = audit_record
        status.pop(_STATUS_AUDIT_ERROR, None)
    if audit_error is not None:
        status[_STATUS_AUDIT_ERROR] = audit_error
    body["status"] = status

    meta = _meta(body)
    if not meta.get("uid") or not meta.get("resourceVersion"):
        raise KubernetesError(
            "InvalidKubernetesResponse",
            "BackupPolicy is missing metadata.uid/resourceVersion; cannot update status",
        )

    try:
        return client.replace_policy_status(namespace, name, body)
    except Conflict as exc:
        raise AuditRejected(
            "ConcurrentAuditUpdate",
            "status was modified by another writer before this audit could be "
            f"written ({exc.message}); refusing to overwrite it",
        ) from exc


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------

def _baseline_of(policy: dict, namespace: str, name: str) -> _Baseline:
    meta = _meta(policy)
    uid = meta.get("uid")
    if not isinstance(uid, str) or not uid:
        raise KubernetesError(
            "InvalidKubernetesResponse",
            f'BackupPolicy "{namespace}/{name}" is missing metadata.uid',
        )
    return _Baseline(
        uid=uid,
        generation=_generation_of(policy),
        audit_run_id=_audit_run_id(policy),
    )


def _generation_of(policy: dict) -> int:
    generation = _meta(policy).get("generation")
    if generation is None:
        return 0
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise KubernetesError(
            "InvalidKubernetesResponse",
            f"BackupPolicy metadata.generation must be a non-negative integer, got {generation!r}",
        )
    return generation


def _audit_run_id(policy: dict) -> object:
    audit = _status_of(policy).get(_STATUS_AUDIT)
    if isinstance(audit, dict):
        return audit.get("runID")
    return None


def _status_of(obj: dict) -> dict:
    status = obj.get("status")
    if status is None:
        return {}
    if not isinstance(status, dict):
        raise KubernetesError(
            "InvalidKubernetesResponse",
            "BackupPolicy status must be an object",
        )
    return status


def _meta(obj: dict) -> dict:
    meta = obj.get("metadata")
    return meta if isinstance(meta, dict) else {}


def _timestamp(now: datetime | None) -> str:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
