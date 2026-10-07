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
| `MALFORMED_JSON` | 整条报文格式不合法：非 JSON、畸形 JSON、或 JSON 不是对象 |
| `MISSING_FIELD` | 必填字段缺失 / 空字符串 |
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

### 拒绝留痕与自查

每一次被拒绝的上报都会留痕（`RejectionRecord`：序号、管道名、拒绝原因、说明、
原文片段、拒绝时刻），管道名从原始报文中尽力提取，解析不出时归入 `<unknown>`。
留痕与计数均为线程安全的原子累加，并发被拒不丢不重。

**留痕容量上限（明细有界、计数精确）**：留痕明细保存在容量上限为
`observability.rejection-log-capacity`（默认 10000 条）的有界队列中，超出后按到达
先后淘汰最旧明细（FIFO），内存不随接入方与拒绝量无限增长。按管道、按原因的累计
被拒条数与明细存储解耦、只增不减——明细被淘汰后计数一条不少、始终精确；
按管道/原因的明细查询照常可用（返回仍保留的明细），汇总查询始终反映全量累计。

- `GET /api/observability/rejections?pipeline=<名>&code=<原因>`：按管道、按原因过滤
  查询留痕明细（参数均可省略；仅返回容量内仍保留的明细）。
- `GET /api/observability/rejections/summary?pipeline=<名>`：每种原因各自累计被拒
  条数（`byCode`）与总数（`total`，全量累计、不受淘汰影响）；省略 `pipeline`
  时统计全局。响应另带 `retained`（当前保留明细条数）与 `capacity`（容量上限），
  用于核对淘汰语义。

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
- **调度周期（按配置的间隔推算下一轮）**：每个管道按 `scheduleIntervalMs` 推算
  “下一轮本该什么时候跑”：`expectedNextRun = max(最近成功, 最近起点, 已见调度时刻)
  + scheduleIntervalMs`。判定宽限取 `max(scheduleGraceMs, jitterGraceMs)`，
  保证与新鲜度抖动带同一口径、互不矛盾：
  - `now <= expectedNextRun` → 两轮之间还没到点，正常，**不出正式告警**；
  - `expectedNextRun < now <= expectedNextRun + 宽限` → 正常范围内的延迟，
    `TRANSIENT_JITTER`，**不出正式告警**（边界含等号，踩在周期/宽限边界上不误报）；
  - `now > expectedNextRun + 宽限` → 已错过调度，判 `SCHEDULE_LAGGING` 并告警。
    错过轮数 `missedRunCount` = 满足 `anchor + k*interval + 宽限 < now` 的轮数
    （第 k 轮的宽限边界含等号，恰好踩界不计为错过）；连续错过轮数
    `>= criticalMissedRuns` 时告警直接以 **CRITICAL** 触发/升级（错过越多越严重）。
  - 评估结果携带 `expectedNextRunMs / observedLagMs / missedRunCount /
    suggestedSeverity` 字段，`reasons` 中给出完整推算过程。
  - 新鲜度已 `STALE` 时以 `STALE` 为准（缺轮信息仍记录在字段中）。
  - 把 `scheduleIntervalMs` 调大或调小，期望时刻与缺轮判定随之变化；
    刚刚补跑成功的管道锚点立即刷新、缺轮清零，告警同口径关闭。
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

### 结论与告警同口径的保证

无论结论由上报联动算出、还是由查询/对账触发，同一时刻对同一管道的健康结论与
告警状态都一致，且恢复不留残留、不重复通知：

- **统一服务时钟**：上报接入、联动评估、告警触发/恢复与查询/对账全部使用同一个
  可替换的 `SystemClock`，不存在“联动用墙钟、查询用另一套时间”的双口径；
  本地把服务时间固定下来验证时，各路径结论必然相同，恢复指令也不会被
  “迟到保护”误吞（这正是之前告警恢复不掉的根因）。
- **结论不含即恢复**：每次健康评估联动告警对账时，结论不产生哪类健康告警
  （STALE / SCHEDULE_LAGGING / SLA_BREACHED），哪类活跃告警就当场幂等恢复；
  结论为 HEALTHY 时连 RUN_FAILED 也一并关闭。因此“说正常就不挂任何告警”，
  结论在告警类型之间切换（如 SCHEDULE_LAGGING → STALE）也不会残留旧类型告警。
- **恢复可追溯**：恢复的告警从活跃列表移除并记入历史
  （`GET /api/observability/alerts?includeRecovered=true` 可查 RECOVERED 记录），
  此前真正对外通知过的告警恢复时发恰好一条 RECOVERY 通知；重复恢复是空操作。

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
| GET  | `/api/observability/rejections?pipeline=&code=` | 被拒上报留痕查询（可按管道/原因过滤） |
| GET  | `/api/observability/rejections/summary?pipeline=` | 各拒绝原因累计被拒条数 |
| GET  | `/api/observability/pipelines/{pipeline}/reconciliation` | 单管道运行质量与告警对账 |
| GET  | `/api/observability/snapshot` | 健康+告警+维护窗口总览 |

### 对账接口（同一时刻的一致快照，结果可自证自洽）

`GET /api/observability/pipelines/{pipeline}/reconciliation` 返回：

- `health`：当前健康结论（含判定依据 `reasons` 与调度推算字段）；
- `activeAlerts`：对账后的活跃告警——与健康结论同一时刻、同一口径
  （HEALTHY 时活跃告警必为空；异常时对应告警必在挂）；
- 计数：`receivedEvents`（接收总数）、`duplicateEvents`（精确重复）、
  `lateEvents`（迟到忽略）、`appliedEvents`（= received − duplicate − late）、
  `rejectedEvents` 与 `rejectionsByCode`（被拒总数及分原因计数）、
  `alertMergedCount`（告警合并计数）、`runsTracked`。
  所有计数在并发/乱序/重复下原子累加，精确不丢不重。
- `consistent` 与 `consistencyNotes`：**自洽声明及其判断依据**。

**一致快照如何保证**：上报的“状态收敛 + 联动健康评估/告警对账”与按管道对账在
同一把一致性守卫（`ConsistencyGuard`）内互斥执行，因此一次对账里的健康结论、
告警状态与各类计数都来自同一时刻的快照——上报与对账并发打进来时，也不会拿到
结论与告警互相矛盾的组合。

**`consistent=true` 的含义**：返回前对快照逐项校验并全部通过，依据逐条写入
`consistencyNotes`：计数勾稽（`applied = received − duplicate − late`）、
被拒汇总勾稽（`rejectedEvents = Σ rejectionsByCode`）、健康结论与活跃告警同口径
（结论产生的告警类型必在挂、不产生的必不在挂；HEALTHY 时活跃告警为空）。
若校验发现矛盾，服务会重取一次快照再校验；仍矛盾则返回 **409 CONFLICT**
（响应体含矛盾点 `violations`），绝不把矛盾结果悄悄返回。

## 6. 可配置项（`src/main/resources/application.properties`）

| 配置键 | 默认值 | 含义 |
|--------|--------|------|
| `observability.supported-protocol-versions` | 1.0, 1.1 | 允许的协议版本 |
| `observability.default-freshness-threshold-ms` | 3600000 | 新鲜度阈值 |
| `observability.default-jitter-grace-ms` | 300000 | 抖动宽限 |
| `observability.default-sla-limit-ms` | 7200000 | SLA 上限 |
| `observability.default-schedule-interval-ms` | 3600000 | 调度间隔（推算下一轮本该运行的时刻） |
| `observability.default-schedule-grace-ms` | 120000 | 调度滞后宽限 |
| `observability.escalation-after-ms` | 1800000 | 告警升级时长 |
| `observability.critical-missed-runs` | 2 | 连续错过多少轮调度即视为严重（告警直接 CRITICAL） |
| `observability.rejection-log-capacity` | 10000 | 被拒留痕明细容量上限（条），超出按到达先后淘汰最旧；累计计数不受影响 |
| `observability.pipelines.<name>.*` | - | 按管道覆盖以上阈值（如 `schedule-interval-ms`、`freshness-threshold-ms`） |

## 7. 验证方法

```bash
mvn test                 # 全部 36 个单测/集成测试
mvn spring-boot:run      # 启动服务，默认 8080 端口
```

测试使用可拨动的 `MutableClock` 固定/推进时间，覆盖：

- `EventIngestionValidationTest`：五类字段级拒绝原因 + 合法事件；
- `RejectionAuditTest`：六类拒绝原因可区分、全部留痕、按管道/原因查询、
  各原因计数精确、被接受事件不留痕；
- `StateConvergenceTest`：精确重复、乱序终结、终结态不回退、起点乱序修正；
- `ConcurrentIngestionTest`：16 线程 × 100 并发事件，计数不丢失、结论唯一、告警合并为一条；
- `ConcurrentExactCountTest`：8 线程并发混合流量（合法/重复/迟到/非法），
  接收/重复/迟到/被拒计数精确，并发对账快照恒满足
  `received == applied + duplicate + late` 且健康与告警口径自洽；
- `FreshnessSlaBoundaryTest`：新鲜度等号边界、抖动带、NEVER_RAN、刚恢复不误报、
  SLA 等号/越界、调度滞后宽限边界；
- `ScheduleIntervalBoundaryTest`：按配置调度周期推算下一轮、周期/宽限等号边界不误报、
  连续缺轮计数与 CRITICAL 升级、周期调大调小结论跟着变、补跑后立即恢复；
- `AlertLifecycleTest`：重复合并只通知一次、超时升级、恢复无残留不重复通知、
  维护窗口抑制与窗口后补发；
- `ReconciliationConsistencyTest`：对账视图的健康结论与告警状态同口径
  （STALE 时告警在挂、恢复后无残留），计数与实际上报逐条对得上；
- `AlertRecoveryConsistencyTest`：固定服务时间下，上报联动/主动查询/对账三条路径
  结论一致；恢复正常后告警当场恢复（活跃无残留、历史有 RECOVERED、通知恰好一次、
  重复恢复空操作）；TRANSIENT_JITTER 结论同样清除已挂健康类告警；
- `ReconciliationSnapshotConsistencyTest`：4 写线程 + 3 读线程并发打满，
  480 份并发对账快照逐份校验自洽（consistent、计数勾稽、结论与告警同口径），
  终态计数精确；
- `RejectionLogCapacityTest`：留痕超容量后最旧明细按 FIFO 淘汰、保留明细有序，
  按管道/原因累计计数一条不少，淘汰后明细查询与汇总查询照常可用；
  8 线程 × 500 并发被拒下容量与计数同时成立；
- `WebApiIntegrationTest`：HTTP 端到端、畸形 JSON 结构化拒绝（MALFORMED_JSON）、
  拒绝留痕查询/汇总接口、对账接口。

所有关键测试均通过 SLF4J 打印 `[INPUT]/[RESULT]/[CHECK]/[ASSERT]` 等关键输入与判定依据。

快速手工验证：
```bash
# 上报一条失败事件并查看告警
curl -s -XPOST localhost:8080/api/observability/events -H 'Content-Type: application/json' \
  -d '{"protocolVersion":"1.0","runId":"r1","pipelineName":"etl","status":"FAILED","eventTime":1700000000000}'
curl -s localhost:8080/api/observability/alerts

# 制造一条被拒上报并自查留痕与分原因计数
curl -s -XPOST localhost:8080/api/observability/events -H 'Content-Type: application/json' \
  -d '{"protocolVersion":"9.9","runId":"r2","pipelineName":"etl","status":"RUNNING","eventTime":1700000000000}'
curl -s 'localhost:8080/api/observability/rejections?pipeline=etl'
curl -s 'localhost:8080/api/observability/rejections/summary?pipeline=etl'

# 查看单管道对账（健康结论 + 告警 + 计数，同一时刻一致快照，含 consistent/consistencyNotes）
curl -s localhost:8080/api/observability/pipelines/etl/reconciliation

# 留痕容量：把 observability.rejection-log-capacity 调小（如 10）重启，
# 连续触发若干次被拒上报后观察——明细只保留最新 10 条，summary 的 total/byCode 仍是全量累计
curl -s 'localhost:8080/api/observability/rejections/summary'   # 含 retained 与 capacity
```

本地验证“结论与告警同口径 / 恢复不掉”场景：测试时钟固定（`MutableClock`）时，
上报联动、主动查询与对账走同一时钟，管道补跑成功后告警当场恢复；
对应自动化用例见 `AlertRecoveryConsistencyTest`。并发“上报 + 对账”下快照自洽
见 `ReconciliationSnapshotConsistencyTest`，留痕淘汰与计数精确见
`RejectionLogCapacityTest`，均可通过 `mvn test` 复现，日志中带
`[INPUT]/[RESULT]/[ASSERT]` 等输入与判定依据。
