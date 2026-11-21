"""盘点所需的最小 Kubernetes API 客户端（同步 httpx）。

只实现本仓库用到的三类请求：

* 读取 BackupPolicy（存储版本 v1beta1）；
* 分页读取 VolumeSnapshot；
* 以 ``resourceVersion`` 做乐观并发控制，PUT BackupPolicy 的 ``/status`` 子资源。

错误按 Kubernetes Status 的语义分成 NotFound / Conflict / ContinueTokenExpired，
方便盘点层决定"保留上次结果"还是"拒绝写回"。
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import httpx

GROUP = "backup.example.com"
POLICY_API_VERSION = f"{GROUP}/v1beta1"
SNAPSHOT_API_VERSION = "snapshot.storage.k8s.io/v1"

DEFAULT_API_URL = "http://127.0.0.1:8081"
#: 真实集群里 ServiceAccount token 由 kubelet 挂载并轮换；只读一次会在轮换后 401。
SERVICE_ACCOUNT_TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"


class KubernetesError(Exception):
    """对 Kubernetes API 的请求失败。"""

    def __init__(self, reason: str, message: str, status_code: int | None = None):
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.status_code = status_code


class NotFound(KubernetesError):
    pass


class Conflict(KubernetesError):
    """resourceVersion / UID 不匹配，说明对象被并发修改或同名重建。"""


class ContinueTokenExpired(KubernetesError):
    """分页 continue token 已过期（HTTP 410）。"""


class KubernetesClient:
    """BackupPolicy / VolumeSnapshot 客户端。

    ``client`` 可注入一个已配置 base_url 的 ``httpx.Client``，主要供测试使用。
    """

    def __init__(
        self,
        base_url: str = DEFAULT_API_URL,
        *,
        token: str | None = None,
        token_file: str | None = None,
        ca_file: str | None = None,
        timeout: float = 10.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._token = token
        self._token_file = token_file
        verify: bool | str = ca_file or True
        self._client = client or httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers={"Accept": "application/json"},
            verify=verify,
        )
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> KubernetesClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- BackupPolicy -----------------------------------------------------

    def get_policy(self, namespace: str, name: str) -> dict:
        return self._request("GET", self._policy_path(namespace, name))

    def replace_policy_status(self, namespace: str, name: str, policy: dict) -> dict:
        """PUT /status 子资源；body 必须带 metadata.resourceVersion 与 metadata.uid。"""
        return self._request(
            "PUT", f"{self._policy_path(namespace, name)}/status", json=policy
        )

    # -- VolumeSnapshot ---------------------------------------------------

    def list_volumesnapshots(
        self, namespace: str, limit: int, continue_token: str | None = None
    ) -> dict:
        params: dict[str, object] = {"limit": limit}
        if continue_token:
            params["continue"] = continue_token
        return self._request(
            "GET",
            f"/apis/{SNAPSHOT_API_VERSION}/namespaces/{quote(namespace, safe='')}/volumesnapshots",
            params=params,
        )

    # -- 内部 -------------------------------------------------------------

    @staticmethod
    def _policy_path(namespace: str, name: str) -> str:
        return (
            f"/apis/{POLICY_API_VERSION}/namespaces/{quote(namespace, safe='')}"
            f"/backuppolicies/{quote(name, safe='')}"
        )

    def _auth_headers(self) -> dict[str, str]:
        """每次请求前重读 token 文件，跟上 kubelet 的轮换。"""
        token = self._token
        if self._token_file:
            try:
                token = Path(self._token_file).read_text(encoding="utf-8").strip() or token
            except OSError:
                pass  # 文件暂不可读时退回环境变量/构造参数里的 token
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _request(self, method: str, path: str, **kwargs: object) -> dict:
        headers = {**kwargs.pop("headers", {}), **self._auth_headers()}  # type: ignore[arg-type]
        try:
            response = self._client.request(method, path, headers=headers, **kwargs)
        except httpx.HTTPError as exc:
            raise KubernetesError("KubernetesAPIConnectionFailed", str(exc)) from exc

        if response.status_code >= 400:
            raise _error_from_response(response)
        try:
            body = response.json()
        except ValueError as exc:
            raise KubernetesError(
                "InvalidKubernetesResponse",
                f"Kubernetes API returned non-JSON body for {method} {path}",
                response.status_code,
            ) from exc
        if not isinstance(body, dict):
            raise KubernetesError(
                "InvalidKubernetesResponse",
                f"Kubernetes API returned a non-object body for {method} {path}",
                response.status_code,
            )
        return body


def _error_from_response(response: httpx.Response) -> KubernetesError:
    reason = ""
    message = response.text or f"HTTP {response.status_code}"
    try:
        body = response.json()
    except ValueError:
        body = None
    if isinstance(body, dict):
        reason = str(body.get("reason") or "")
        message = str(body.get("message") or body.get("detail") or message)

    if response.status_code == 404:
        return NotFound(reason or "NotFound", message, 404)
    if response.status_code == 409:
        return Conflict(reason or "Conflict", message, 409)
    if response.status_code == 410:
        return ContinueTokenExpired(reason or "Expired", message, 410)
    return KubernetesError(reason or "KubernetesAPIError", message, response.status_code)
