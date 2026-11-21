# BackupPolicy Conversion Webhook

Kubernetes CRD `backuppolicies.backup.example.com` 的版本转换 webhook（FastAPI 实现）。
集群升级备份控制器后，**v1beta1 是存储版本**，支持为每个备份目标单独设置保留份数；
值班员的旧工具仍只认 **v1alpha1**（只有默认保留份数 + 目标名列表）。本 webhook 让
旧工具在不了解新字段的情况下安全地读、改、写回，且不丢任何 per-target 设置。

## 版本模型

```yaml
# v1beta1（存储版本）
spec:
  defaultRetention: 3
  targets:
    - name: db
      retention: 5        # 可选：单独份数；缺省 = 跟随 defaultRetention
    - name: logs

# v1alpha1（旧版）
spec:
  defaultRetention: 3
  targets: ["db", "logs"]
```

## 转换契约（实现要点）

v1alpha1 表达不了 per-target 设置，往返保真靠对象上的**兼容注解**
`backup.example.com/v1alpha1-compat` 完成：

- **读（v1beta1 → v1alpha1）**：把所有**显式**单独设置汇总成 JSON 写进注解，
  如 `{"retentions":{"cache":3,"db":5},"v":1}`。即使某目标的份数恰好等于当前
  默认值也照样记录——"显式设置"这个事实必须保留，否则旧客户端改默认值后它会
  被错误地变成"跟随默认"。
- **写回（v1alpha1 → v1beta1）**：按**目标名**从注解恢复单独设置；注解里没有的
  目标（新增的、或删除后又同名加回的）不设置份数，即**沿用当前默认值**。注解在
  写回时被摘除，不进入存储版本。目标删除后再同名添加会丢设置，是因为每次读取
  都按存储对象重新生成注解——删过的目标自然不在里面。
- **无状态**：往返所需的全部信息都在注解里，两次转换之间重启 webhook 不影响写回。
- **失败语义**：
  - 旧版**新建**（无 `resourceVersion`）：免注解，正常转换；
  - 带 `resourceVersion` **写回**时注解**缺失或损坏**：整个 ConversionReview
    返回 `Failure`（HTTP 200 + `response.result`），`convertedObjects` 为空，
    消息逐个指出问题对象（序号 + `namespace/name` + 原因）——批内合法对象也
    不会被转换，保证整批原子性；
  - 注解损坏判定：非法 JSON、payload 版本不符、`retentions` 非对象、份数为
    负数/非整数/布尔等；
  - 附带防御性校验：目标名重复、类型错误等同样整批失败。

## 盘点 API（快照保留缺口）

备份控制器需要知道"每个目标还缺几份快照"。新增按 namespace/name 发起的盘点：

```http
POST /audit/{namespace}/{name}?page_size=50
```

一轮盘点的流程与语义：

- **分页读取**同命名空间的 VolumeSnapshot（`limit` + `continue`），逐页累积；
  所有页的 `metadata.resourceVersion` 必须一致，**收齐同一份清单**才写 `/status`。
- **只统计**满足全部条件的快照：
  - `metadata.ownerReferences` 里有属主 `uid` **等于 BackupPolicy UID**；
  - `status.readyToUse` 严格为 `true`；
  - 没有 `metadata.deletionTimestamp`（未进入删除）；
  - 带 `backup.example.com/target` 注解（注解值即目标名）。
- **写 `/status`**（子资源，带 `resourceVersion` + `uid`）新增：
  - `status.audit`：`runID`、`policyUID`、策略 `generation`、清单
    `snapshotResourceVersion`、`completedAt`，以及每个目标的
    `retention` / `available` / `gap`（`gap = max(retention - available, 0)`，
    `retention` 单独设置优先，否则 `defaultRetention`）；
  - `status.auditError`：读取失败时记录原因；
  - 其余 status 字段（`observedGeneration`、`message`、`conditions`…）**原样保留**；
  - CRD 两个版本的 status schema 都声明了这两个字段，所以 v1alpha1 与 v1beta1
    都能读到同一份盘点结果。
- **续页过期（410）或任何读取失败**：不写新结果，保留上次 `status.audit`，
  只写 `status.auditError`（`reason` + `message` + `at`），响应 `outcome=stale`。
- **盘点中策略被改**：不重扫快照，拿已经收齐的同一份清单按现行策略重算，
  响应 `recomputed=true`、`generation` 为最新值。
- **拒绝写回（409）**：
  - 策略同名重建（UID 变化）→ `PolicyRecreated`；
  - 另一轮盘点已经更新 `status.audit`（runID 变化）→ `ConcurrentAuditUpdate`；
  - 写 `/status` 时 resourceVersion 冲突 → `ConcurrentAuditUpdate`。
- 其他错误：策略不存在 404；spec 不合法 422；Kubernetes API 不可达/非快照读失败 502。

### 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `KUBERNETES_API_URL` | `http://127.0.0.1:8081` | Compose 指向 mockkube；真实集群用 `https://kubernetes.default.svc` |
| `KUBERNETES_TOKEN` | 无 | 显式 bearer token |
| `KUBERNETES_TOKEN_FILE` | In-cluster SA token 路径 | 每次请求前重读，跟上 kubelet 轮换 |
| `KUBERNETES_CA_FILE` | In-cluster `ca.crt`（仅 HTTPS 时） | 校验 API server 证书 |

## 目录

```
app/conversion.py   转换逻辑（纯函数、无状态）
app/audit.py        盘点逻辑：分页收齐、过滤、缺口计算、status 并发语义
app/kube.py         最小 Kubernetes 客户端（BackupPolicy / VolumeSnapshot / status）
app/main.py         FastAPI：POST /convert、POST /audit/...、GET /healthz
app/examples.py     Swagger 演示 / 测试共用的示例载荷
app/mockkube.py     本地 Kubernetes 模拟端（分页、410、并发场景注入）
manifests/crd-backuppolicy.yaml      CRD（v1beta1 存储、webhook 转换、status schema）
manifests/webhook-deployment.yaml    可选：真实集群部署（RBAC 见下）
scripts/demo.py     对运行中的服务实测转换全流程
scripts/audit_demo.py  对运行中的服务实测盘点全流程（含并发场景）
tests/              pytest（63 例：25 转换 + 38 盘点）
Dockerfile / docker-compose.yaml     本地一键启动
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 盘点 API：   http://localhost:8080/docs  （POST /audit/default/web-backup）
# 模拟 K8s：   http://localhost:8081/docs  （/_mock/* 制造续页过期、改策略等）
```

在 Swagger 里对本地模拟端发起盘点：

1. 打开 `http://localhost:8080/docs`，执行 `POST /audit/{namespace}/{name}`，
   `namespace=default`、`name=web-backup`、`page_size=2`；
   响应与 `status.audit` 给出 `db 可用 2/保留 5/缺 3`、`cache 3/3/0`、
   `logs 1/3/2`、`metrics 0/2/2`；
2. 在 mockkube 的 Swagger 里先 `POST /_mock/schedule`（`action=expire_continue_tokens`,
   `afterListRequest=1`）再盘点 → `outcome=stale`，上次结果保留、`auditError.reason=Expired`；
3. 先调度 `action=bump_generation` + 新 spec 再盘点 → `recomputed=true`，
   同一份清单按新策略重算；调度 `recreate_policy` 或 `set_audit_run_id` → 409 拒绝写回。

一键脚本（Compose 起来后）：

```bash
.venv/bin/python scripts/audit_demo.py    # 默认连 8080 / 8081
```

### 转换演示（Swagger）

在 `POST /convert` 的 **Example** 下拉框里按编号演示"旧客户端读 → 改 → 写回"：

1. **① 读取**：v1beta1 → v1alpha1，per-target 设置收进兼容注解
   （`cache` 的 3 恰好等于默认值，也照样记录）；
2. **② 写回：改默认值 + 重排目标**：默认值 3→10、目标重排为 `[logs, cache, db]`，
   响应中 `db=5`、`cache=3` 按名字恢复为显式设置（`cache` 不跟随新默认 10），
   注解被摘除；
3. **③ 删除后同名重加**：`cache` 被删过一轮再加回 → 沿用当前默认，不带份数；
4. **④ 丢注解 / ⑤ 注解损坏**：带 resourceVersion 写回 → 整批 Failure 并指名；
5. **⑥ 旧客户端新建**：无 resourceVersion，免注解。

**重启验证**：执行①后 `docker compose restart webhook`，再执行②，结果不变。

不用 Docker 也可以本地跑：

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/uvicorn app.mockkube:app --port 8081 &          # 模拟 K8s
KUBERNETES_API_URL=http://127.0.0.1:8081 \
  .venv/bin/uvicorn app.main:app --port 8080 &            # 盘点/转换 API
.venv/bin/python scripts/demo.py                          # 转换全流程
.venv/bin/python scripts/audit_demo.py                    # 盘点全流程（含并发场景）
.venv/bin/python -m pytest                                # 63 个测试
```

## 部署到真实集群

```bash
kubectl apply -f manifests/crd-backuppolicy.yaml        # caBundle 见下
kubectl apply -f manifests/webhook-deployment.yaml      # 可选：webhook 本体
```

- `spec.conversion.webhook.clientConfig.caBundle` 需填入签发 webhook 证书的 CA
  （建议 cert-manager，CRD 顶部有注入注释）；Compose 演示不经过 API server，用不到。
- webhook 以 HTTPS 暴露 `/convert`（部署清单里 uvicorn 直接持证书，Service 443→8443）。
- 盘点 API 需要 RBAC（`manifests/webhook-deployment.yaml` 已包含）：对
  `backuppolicies` 的 `get`，对 `backuppolicies/status` 的 `update`，对
  `volumesnapshots` 的 `list`；并把 `KUBERNETES_API_URL` 指向
  `https://kubernetes.default.svc`。

## 自测结果（本仓库交付前实测）

- `pytest`：**63 passed**（25 个转换/协议用例 + 38 个盘点用例）。转换侧覆盖：
  读取收注解（含"等于默认也记录"）、改默认值 + 重排写回、增删目标、删后同名重加
  跟随当前默认、重启后写回、丢注解/注解损坏（8 种变体）整批失败并指名、旧版新建、
  重复目标拒绝、往返幂等、同版本透传、未知版本拒绝、uid 回显、健康检查。
  盘点侧覆盖：page_size 1/2/3/5/50 的分页收齐、属主 UID/ready/删除中/缺注解逐条
  过滤、status 其他字段保留、v1beta1 与 v1alpha1 双版本可读、续页 410、续页 500、
  连接失败、分页 RV 不一致中止、盘点中改策略不重扫只重算、同名重建/删除 409、
  另一轮已更新 409、写回 409、非法 spec（负数/类型错/缺默认）422、策略不存在 404、
  K8s 不可达 502、OpenAPI 文档包含盘点端点、成功盘点会清掉上次 auditError。
- 实跑 `uvicorn` + `scripts/demo.py`：读 → 改默认值 3→10 + 重排 → 写回 →
  删 cache 写回 → 重读 → 同名加回 → 跟随默认；丢注解写回返回
  `conversion failed for 1 of 2 object(s): [1] BackupPolicy "default/web-backup":
  missing compatibility annotation ...`，合法对象不被转换。**全部断言通过**。
- **重启实测**：读取后 `kill` 掉 uvicorn 再重启，用重启前拿到的视图写回，
  转换成功且结果正确。
- 实跑 `uvicorn`（8080 API + 8081 mockkube）→ `scripts/audit_demo.py`：
  正常盘点（分页 2）→ 续页过期保留旧结果 → 盘点中改默认值 3→6 / metrics 设 4
  按同一份清单重算（清单 RV 仍为 5000）→ 同名重建、另一轮更新、写回冲突均 409。
  **全部断言通过**。
- `docker-compose.yaml`、两份 manifest 均通过 YAML 解析校验；CRD 的
  served/storage 与 webhook 路径断言正确。

## 未完成 / 限制

- 本开发环境没有 Docker/容器运行时，`docker compose up` 未实际执行；Compose 场景
  已用等价的 uvicorn 直跑（8080 + 8081 两个进程，含进程重启）验证，
  Compose/Dockerfile 经静态校验与人工核对。
- 未在真实 Kubernetes 集群做端到端验证（需 API server 回调 + TLS 证书）；
  `caBundle` 是占位符，上集群前必须注入真实 CA。
- 旧客户端若绕过"先读后写"（如用静态文件 apply 覆盖已有对象），会因缺注解被
  拒绝——这是需求要求的行为，但上线前需告知值班员正确姿势。
- 新建请求若带合法注解会按注解恢复（方便克隆对象），带坏注解则拒绝；
  该行为是本实现的自定义取舍，需求未明确。
- 仅声明支持 ConversionReview `v1`（对 `v1beta1` 形状的请求宽容回显，未正式支持）。
- 盘点只做单轮：没有定时/队列，需要控制器或调用方按周期触发；跨轮次的去重、
  限流、任务持久化不在本次范围。
- mockkube 是内存实现，重启后回到演示数据；它只模拟盘点涉及的接口，
  不是完整 API server。
- 盘点写 `status.auditError` 时同样走 `/status`，若这一写也冲突，会返回 409 并
  保留旧结果（错误不会丢，但调用方需重试）。
