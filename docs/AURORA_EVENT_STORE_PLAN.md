# Aurora PostgreSQL Authoritative Event Store 计划

## 结论

第一版云端同步采用 **Aurora PostgreSQL authoritative event store + control-plane API + S3 derivative bucket**。

本地 iOS/macOS SQLite 只承担：

- 离线 outbox。
- 本地 replicated event cache。
- UI projection / materialized view。
- 可丢弃 derivative cache metadata。

AWS 上的 Aurora PostgreSQL 才是跨设备同步的 authoritative event store。设备不能直连 Aurora；所有写入和读取都必须经过 control-plane API。

## 为什么不用 DynamoDB 做第一版主库

DynamoDB 可以实现 event store，但第一版不选它，原因是我们的核心需求更偏向关系型事件存储：

- 需要每个 library 内稳定、直观的 `global_seq`，方便 replay、cursor、debug 和人工修复。
- 需要同时保证 `op_id` 幂等、`library_id + device_id + device_seq` 唯一、事件 append 和可选 projection 更新。
- 需要经常按 `library_id`、`entity_type + entity_id`、`op_type` 做历史查询和排障。
- event payload 会版本化演进，PostgreSQL 的 `jsonb`、约束、索引和 ad-hoc SQL 更适合。
- 后续很可能维护服务端 read model，例如 derivative metadata、archive receipts、trash state、conflict queue。

DynamoDB 更适合极高吞吐、查询模式非常固定、团队愿意承担单表设计和 cursor 编码复杂度的场景。当前照片库同步更重视 replay 正确性、可理解性和可维护性。

## 目标架构

```text
iOS/macOS app
  -> local command layer
  -> local SQLite outbox + replicated event cache + projection
  -> HTTPS control-plane API
  -> Aurora PostgreSQL authoritative event store
  -> service-side projections / derivative metadata
  -> S3 thumbnail/preview objects
```

原片 canonical store 仍然是单一 original server/NAS，不进入 S3。

## 云端数据模型

### `ledger_events`

权威事件表。所有设备 replay 的真源。

```sql
create table ledger_events (
  library_id text not null,
  global_seq bigint generated always as identity,
  op_id uuid not null,
  device_id text not null,
  device_seq bigint not null,
  hybrid_logical_time jsonb not null,
  actor_id text not null,
  entity_type text not null,
  entity_id text not null,
  op_type text not null,
  payload jsonb not null,
  payload_hash text not null,
  base_version text,
  committed_at timestamptz not null default now(),

  primary key (library_id, global_seq),
  unique (op_id),
  unique (library_id, device_id, device_seq)
);

create index ledger_events_entity_idx
  on ledger_events (library_id, entity_type, entity_id, global_seq);

create index ledger_events_op_type_idx
  on ledger_events (library_id, op_type, global_seq);
```

说明：

- `global_seq` 是服务端 commit 顺序；客户端 pull cursor 只在成功 replay `GET /ops` 返回页后推进。
- `op_id` 用于幂等，但只有在 library + operation identity 与 payload 都一致时才可视为重试成功。
- `library_id + device_id + device_seq` 用于防止同一设备序列重复或乱写。
- `payload_hash` 用于重复提交时判断 payload 是否完全一致。
- 不要求不可篡改性；第一版不做 hash chain，不做 cryptographic verification。

### `device_states`

记录设备同步状态和 heartbeat。

```sql
create table device_states (
  library_id text not null,
  device_id text not null,
  actor_id text not null,
  last_seen_at timestamptz not null,
  last_uploaded_device_seq bigint not null default 0,
  last_pull_cursor bigint not null default 0,
  capabilities jsonb not null default '{}'::jsonb,

  primary key (library_id, device_id)
);
```

### `derivative_objects`

服务端 derivative metadata projection。事件真源仍是 `ledger_events` 里的 `derivative_declared`。

```sql
create table derivative_objects (
  library_id text not null,
  asset_id uuid not null,
  role text not null,
  file_object jsonb not null,
  s3_bucket text not null,
  s3_key text not null,
  s3_etag text,
  pixel_width integer not null,
  pixel_height integer not null,
  declared_event_seq bigint not null,
  updated_at timestamptz not null default now(),

  primary key (library_id, asset_id, role)
);
```

### `archive_receipts`

服务端 archive receipt projection。事件真源仍是 `ledger_events` 里的 `original_archive_receipt_recorded`。

```sql
create table archive_receipts (
  library_id text not null,
  asset_id uuid not null,
  file_object jsonb not null,
  server_placement jsonb not null,
  receipt_event_seq bigint not null,
  committed_at timestamptz not null,

  primary key (library_id, asset_id, receipt_event_seq)
);
```

### `sync_conflicts`

第一版可以先只记录服务端可判定的协议冲突。语义冲突仍由客户端 projector 或后续 read model 处理。

```sql
create table sync_conflicts (
  id uuid primary key,
  library_id text not null,
  entity_type text not null,
  entity_id text not null,
  conflict_type text not null,
  left_op_id uuid,
  right_op_id uuid,
  detail jsonb not null,
  created_at timestamptz not null default now()
);
```

## Control-plane API

### `POST /libraries/{libraryID}/ops`

上传本机 outbox 事件。

请求：

```json
{
  "operations": [
    {
      "opID": "uuid",
      "libraryID": "library",
      "deviceID": "device",
      "deviceSequence": 1,
      "hybridLogicalTime": {
        "wallTimeMilliseconds": 1710000000000,
        "counter": 0,
        "nodeID": "device"
      },
      "actorID": "user",
      "entityType": "asset",
      "entityID": "asset-id",
      "opType": "metadata_set",
      "payload": {},
      "baseVersion": null,
      "createdAt": "iso8601"
    }
  ]
}
```

行为：

- 每个 op 在一个数据库事务中 append。
- `op_id` 已存在且 payload hash 相同，且 library / device / device_seq / entity / op identity 一致：返回已有 `global_seq`，视为成功。
- `op_id` 已存在但 payload hash 不同：返回 conflict。
- `library_id + device_id + device_seq` 已存在但不是同一个 op：返回 conflict。
- 成功后返回每个 op 的 `global_seq` 和最新 cursor。

响应：

```json
{
  "accepted": [
    {
      "opID": "uuid",
      "globalSeq": 123,
      "status": "committed"
    }
  ],
  "cursor": "123"
}
```

### `GET /libraries/{libraryID}/ops?after=<cursor>`

按服务端 commit 顺序拉取事件。

行为：

- `cursor` 是上一轮返回的 `global_seq`。
- 查询 `global_seq > cursor`。
- 按 `global_seq ASC` 返回。
- 分页大小第一版固定，例如 500。

响应：

```json
{
  "operations": [],
  "cursor": "456",
  "hasMore": false
}
```

### `POST /devices/{deviceID}/heartbeat`

上报设备在线状态和持有的 file placements 摘要。

第一版只需要：

- `libraryID`
- `deviceID`
- `actorID`
- app version
- local pending count
- optional placement summary

### `POST /derivatives/uploads`

请求 thumbnail/preview 上传地址。

行为：

- control-plane 校验 library/device/asset 权限。
- 生成 S3 object key。
- 返回 presigned upload URL。
- 上传成功后，客户端再提交 `derivative_declared` event。

### `GET /derivatives/{assetID}?role=thumbnail|preview`

获取 derivative metadata 和 presigned download URL。

行为：

- 从服务端 `derivative_objects` projection 读最新 pointer。
- 生成短期 download URL。
- S3 key 不是客户端权限边界，客户端不能靠猜 key 访问。

### `POST /archive/receipts`

由 original server/NAS 归档进程提交 receipt。

行为：

- 只允许 server actor 或受信设备调用。
- 写入 `original_archive_receipt_recorded` event。
- 更新 `archive_receipts` projection。

## 后端实现选择

第一版建议使用一个简单 HTTP 服务，不要直接把客户端接到 Aurora。

候选：

- TypeScript + Fastify + Drizzle/Kysely
- Python + FastAPI + SQLAlchemy
- Swift/Vapor 不建议第一版使用，团队和生态成本更高

建议第一版选 TypeScript 或 Python。关键是：

- OpenAPI schema 清楚。
- JSON payload 与 Swift `Codable` 保持兼容。
- DB transaction 和 unique constraint 错误处理清楚。
- 测试里能直接跑 PostgreSQL container 或本地 test database。

## Terraform 落地顺序

### Phase 1: 基础网络和远程状态

- Terraform remote backend：S3 state bucket + lock。
- VPC 或复用默认 VPC 的明确决策。
- 基础 security group。
- AWS 托管参数存放数据库连接材料。

### Phase 2: Aurora PostgreSQL

- Aurora PostgreSQL Serverless v2 cluster。
- 最小容量先保守设置，避免开发期成本失控。
- 数据库初始化 migration。
- 输出 endpoint、database name、连接材料 ARN。

### Phase 3: S3 derivative bucket

- thumbnail/preview bucket。
- bucket encryption。
- Block Public Access。
- CORS 限制。
- lifecycle 第一版不自动删除。

### Phase 4: Control-plane runtime

- API Gateway + Lambda，或 App Runner。
- IAM role 只允许：
  - 访问指定 Aurora 连接材料。
  - 连接数据库。
  - 对 derivative bucket 生成/执行必要对象操作。
- 日志进入 CloudWatch。

### Phase 5: Observability 和 release gate

- health endpoint。
- structured logs。
- request id。
- migration version endpoint。
- smoke test：append op、duplicate op、pull cursor、presigned derivative URL。

## 本地 app 改造

当前本地代码已经有 `SyncControlPlaneHTTPClient` 和 `SyncService`，但需要调整语义：

- 本地 `operation_ledger` 改名或文档标注为 local event cache，不再叫 authoritative ledger。
- 本地上传成功后保存 accepted op 的服务端 `global_seq`；pull cursor 只在成功消费 `GET /ops` 返回页后推进。
- `GET /ops` 拉回的是 authoritative committed events。
- 本地 projection replay 时优先按服务端 `global_seq`；没有 `global_seq` 的本地 pending op 只用于本机乐观 UI。
- 本地 pending op 和 remote committed op 需要 reconcile：
  - 同 `op_id` 回来后标记 acknowledged。
  - 不同 `op_id` 但影响同字段时交给 projector/conflict policy。

## 测试计划

### 数据库测试

- append 新 op 返回递增 `global_seq`。
- 重复 `op_id` 只有在完整 operation identity 与 payload 都一致时才幂等返回原 `global_seq`。
- 重复 `op_id` 不同 payload 返回 conflict。
- 重复 `library_id + device_id + device_seq` 不同 op 返回 conflict。
- `GET after cursor` 顺序稳定、分页稳定。
- JSON payload 版本字段可被保留和 round-trip。

### API 测试

- `POST /libraries/{id}/ops` 成功 append。
- `GET /libraries/{id}/ops?after=0` 可完整 replay。
- `POST /derivatives/uploads` 返回受限 presigned URL。
- `GET /derivatives/{assetID}` 在未声明时返回明确 not found。
- archive receipt 只允许 server actor。

### 多设备同步测试

- Mac 离线评分，iPhone 在线加标签，最终两个事件都 replay。
- 两台设备并发改 rating，服务端都接受，客户端 projector 确定性收敛并记录 conflict。
- A 删除，B 离线仍可见；B 拉取后进入共享回收站；A 恢复后所有设备恢复。
- 同一设备重复上传同一 batch 不产生重复事件。
- 新设备从 `after=0` 拉全量事件后能重建 projection。

### 安全测试

- app 不能直连数据库。
- presigned URL 不暴露原片。
- S3 bucket 不公开。
- Terraform state 不提交。
- 任何测试不得删除、移动或覆盖用户照片原片。

## 迁移计划

1. 保留当前本地 ledger/outbox 实现。
2. 后端上线后，把本地 pending events 上传到 AWS authoritative event store。
3. control-plane 返回 `global_seq`，本地记录 acknowledged。
4. 设备开始从 `after=0` 或已保存 cursor 拉取 committed events。
5. 本地 projection 与当前 UI 表做一致性校验。
6. 校验通过后，把本地 ledger 明确降级为 replicated cache + outbox。

## 完成标准

- Terraform 能创建 Aurora、S3 bucket、control-plane runtime。
- 后端 API 有自动化测试和 smoke test。
- macOS app 能把本地 event 上传到 AWS，并从 AWS 拉回 replay。
- 新设备只靠 AWS event store 和 derivative metadata 能重建资料库索引。
- 原片仍不进入 S3。
- 所有验证命令通过：`swift test`、`swift build`、`./scripts/pre_merge_gate.sh`、Terraform validate、后端测试、control-plane smoke test。
