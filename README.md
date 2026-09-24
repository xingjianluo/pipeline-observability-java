# 数据管道可观测性与告警聚合服务

基于 Spring Boot 3.4 / Java 21 的进程内实现。管道运行事件通过内置 HTTP 接口上报，
服务在内存中完成**状态收敛、健康判定（新鲜度 / 调度滞后 / SLA）、告警聚合**，
使乱序、重复、并发的事件收敛到一致且可解释的结论。无需外部数据库或消息队列。

## 1. 事件协议

`POST /api/observability/events`，请求体为 JSON 对象：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `protocolVersion` | string | 是 | 协议版本，当前支持 `1.0`、`1.1` |
| `runId` | string | 是 | 运行实例 ID，同一 runId 的事件收敛到同一状态 |
| `pipelineName` | string | 是 | 管道名 |
| `status` | string | 是 | `PENDING` / `RUNNING` / `SUCCESS` / `FAILED` / `CANCELLED` |
| `eventTime` | integer(>0) | 是 | 事件发生时刻，epoch 毫秒 |
| `attempt` | positive integer | 否 | 第几次尝试 |
| `scheduledTime` | integer(>0) | 否 | 调度时刻，用于调度滞后判定 |

时间一律采用事件自带的 `eventTime`（业务时钟）做顺序判定，接入时刻仅用于记账，
因此事件乱序到达不影响最终结论。

### 拒绝原因（绝不静默丢弃，HTTP 200/400 均带 `rejectCode`）

| rejectCode | 触发条件 |
|------------|----------|
| `MISSING_FIELD` | 必填字段缺失 / 空字符串 / 请求体不是 JSON 对象 / 畸形 JSON |
| `TYPE_MISMATCH` | 字段类型不符（如 protocolVersion 传数字、eventTime 传小数或字符串） |
| `UNSUPPORTED_PROTOCOL_VERSION` | protocolVersion 不在允许列表 |
| `UNKNOWN_STATUS` | status 不是五个合法枚举之一 |
| `INVALID_TIMESTAMP` | 时间字段非正整数 |

响应示例：
```json
{ "accepted": false, "duplicate": false,
  "rejectCode": "UNSUPPORTED_PROTOCOL_VERSION",
  "message": "protocolVersion '2.0' not supported, allowed=[1.0, 1.1]" }
```

## 2. 运行状态收敛规则

生命周期：`PENDING(0) → RUNNING(1) → SUCCESS / FAILED / CANCELLED(2，终结态)`。

- **精确重复**：同一 `(status, eventTime)` 再次到达 → 只增加 `duplicateCount`，状态不变，
  响应 `accepted=true, duplicate=true`。
- **终结态保护**：一旦进入终结态，更旧的终结事件、或任何非终结事件都判为迟到
  （`lateIgnoredCount++`，响应 message 标注 `late event ignored`），**不回退**；
  仅当到达的终结事件 `eventTime` 更新时才覆盖（例如先到 SUCCESS 后到更晚的 FAILED，
  最终为 FAILED），这是“迟到的最终结论”，不是回退。
- **非终结期**：按生命周期次序推进；次序倒退（RUNNING 后到 PENDING）视为迟到旧状态。
  RUNNING 起点取所有 RUNNING 事件中最早的 `eventTime`，SLA 起算不受乱序影响。
- **并发**：每个 runId 在 `ConcurrentHashMap.compute` 临界区内串行收敛；
  事件总数、重复数、迟到数均原子累加，并发上报不丢失、结论唯一。
- 每个运行保留 `transitionLog`（APPLY / DUP / LATE 明细），可通过
  `GET /api/observability/runs/{runId}` 追溯。

## 3. 健康判定（新鲜度 / 调度滞后 / SLA）

判定结果 `FreshnessStatus`：`NEVER_RAN / HEALTHY / STALE / SCHEDULE_LAGGING /
SLA_BREACHED / TRANSIENT_JITTER`，结果对象 `reasons` 数组给出判定依据。

- **NEVER_RAN**：从未有过成功运行，与“曾经正常而后超期”明确区分。
- **SLA**：最近运行为 RUNNING 时，`now - 最早起点 > slaLimitMs` 判 `SLA_BREACHED`；
  `duration == slaLimit` 不告警（阈值语义为“超过才报”）。
- **调度滞后**：最近运行未开始（无 RUNNING 起点）且带 scheduledTime，
  `now - scheduledTime > scheduleGraceMs` 判 `SCHEDULE_LAGGING`；宽限内为 `TRANSIENT_JITTER`。
- **新鲜度**（距上次成功的时间 age）：
  - `age <= freshnessThreshold` → **HEALTHY（边界含等号，时钟边界不误报）**
  - `threshold < age <= threshold + jitterGrace` → **TRANSIENT_JITTER（短暂抖动，不产生正式告警）**
  - `age > threshold + jitterGrace` → **STALE**
- **刚刚恢复不误报**：FAILED/CANCELLED 之后很快重跑成功，`lastSuccessTime` 立即刷新，
  失败相关告警随之恢复；运行成功结束后 SLA 告警自动解除。

## 4. 告警规则

告警键为 `(pipelineName, alertType)`，类型：`STALE / SCHEDULE_LAGGING / SLA_BREACHED /
RUN_FAILED`；级别 `WARNING → CRITICAL`；状态 `FIRING / SUPPRESSED / RECOVERED`。

- **触发与去重合并**：同一键在恢复前只有一条 Alert，重复上报只累加 `count`，
  只发一次 FIRING 通知；合并在 `ConcurrentHashMap.compute` 内完成，并发计数不丢失。
- **升级**：告警持续 `escalationAfterMs` 后，下一次观测将 WARNING 升级为 CRITICAL，
  发一次 ESCALATION 通知（升级只发生一次）。
- **恢复**：健康判定回到 HEALTHY（或显式调用恢复接口）时告警转为 RECOVERED，
  从活跃列表移除并记入历史（`?includeRecovered=true` 可查），恰好一条 RECOVERY 通知；
  重复恢复为空操作，不重复通知。
- **维护窗口**：窗口内触发的告警置为 SUPPRESSED，计数照常合并（`suppressedCount`）但
  **不发送任何通知**，记录仍可追溯；窗口结束后若仍异常，沿同一条告警补发一次 FIRING。
  窗口可限定管道或全局（pipelineName 省略为全局），时间区间闭区间。
- 迟到保护：`atMillis` 早于已处理时间的观测/恢复指令一律忽略，旧消息不能覆盖新状态。

通知出口为进程内 `InMemoryNotificationSink`，记录 FIRING / ESCALATION / RECOVERY 三类通知。

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/observability/events` | 上报运行事件 |
| GET  | `/api/observability/health/{pipeline}` | 单管道健康判定（含 reasons） |
| GET  | `/api/observability/health` | 全部管道健康判定 |
| GET  | `/api/observability/alerts?includeRecovered=false` | 告警列表 |
| POST | `/api/observability/alerts/{pipeline}/{type}/recover` | 手动恢复告警 |
| POST | `/api/observability/maintenance` | 新增维护窗口 `{pipelineName?,startAt,endAt,note?}` |
| GET  | `/api/observability/maintenance` | 维护窗口列表 |
| GET  | `/api/observability/runs/{runId}` | 单运行收敛状态与转移日志 |
| GET  | `/api/observability/snapshot` | 健康+告警+维护窗口总览 |

## 6. 可配置项（`src/main/resources/application.properties`）

| 配置键 | 默认值 | 含义 |
|--------|--------|------|
| `observability.supported-protocol-versions` | 1.0, 1.1 | 允许的协议版本 |
| `observability.default-freshness-threshold-ms` | 3600000 | 新鲜度阈值 |
| `observability.default-jitter-grace-ms` | 300000 | 抖动宽限 |
| `observability.default-sla-limit-ms` | 7200000 | SLA 上限 |
| `observability.default-schedule-interval-ms` | 3600000 | 调度间隔 |
| `observability.default-schedule-grace-ms` | 120000 | 调度滞后宽限 |
| `observability.escalation-after-ms` | 1800000 | 告警升级时长 |
| `observability.pipelines.<name>.*` | - | 按管道覆盖以上阈值（如 `freshness-threshold-ms`） |

## 7. 验证方法

```bash
mvn test                 # 全部 23 个单测/集成测试
mvn spring-boot:run      # 启动服务，默认 8080 端口
```

测试使用可拨动的 `MutableClock` 固定/推进时间，覆盖：

- `EventIngestionValidationTest`：五类拒绝原因 + 合法事件；
- `StateConvergenceTest`：精确重复、乱序终结、终结态不回退、起点乱序修正；
- `ConcurrentIngestionTest`：16 线程 × 100 并发事件，计数不丢失、结论唯一、告警合并为一条；
- `FreshnessSlaBoundaryTest`：新鲜度等号边界、抖动带、NEVER_RAN、刚恢复不误报、
  SLA 等号/越界、调度滞后宽限边界；
- `AlertLifecycleTest`：重复合并只通知一次、超时升级、恢复无残留不重复通知、
  维护窗口抑制与窗口后补发；
- `WebApiIntegrationTest`：HTTP 端到端、畸形 JSON 结构化拒绝。

所有关键测试均通过 SLF4J 打印 `[INPUT]/[RESULT]/[CHECK]/[ASSERT]` 等关键输入与判定依据。

快速手工验证：
```bash
curl -s -XPOST localhost:8080/api/observability/events -H 'Content-Type: application/json' \
  -d '{"protocolVersion":"1.0","runId":"r1","pipelineName":"etl","status":"FAILED","eventTime":1700000000000}'
curl -s localhost:8080/api/observability/alerts
```
