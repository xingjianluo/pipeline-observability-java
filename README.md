# 数据管道可观测性与告警聚合服务

基于 **Java 21 + Spring Boot 3.4 + 进程内并发状态（ConcurrentHashMap）** 实现的管道可观测性服务。
无需外部中间件：管道运行事件通过 HTTP 上报，服务在内存中完成 **状态收敛、健康判定（新鲜度 /
调度滞后 / SLA / 抖动）、告警触发去重 / 升级 / 恢复 / 维护窗口抑制**，并对非法事件给出
**可区分的拒绝原因**且全部留痕。

---

## 1. 快速开始

```bash
mvn spring-boot:run          # 启动（默认端口 8080）
mvn test                     # 全量测试（含乱序/重复/并发/SLA 边界场景）
```

内置 5s 周期调度（可用配置关闭，改为手动 `POST /api/health/tick` 驱动，测试中用可控时钟）。

---

## 2. 事件协议（v1）

`POST /api/events`，请求体为 JSON 对象（`Content-Type: application/json`）：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `protocolVersion` | string | 是 | 仅支持 `"1.0"`，不兼容版本明确拒绝 |
| `pipelineId` | string | 是 | 管道标识，非空 |
| `runId` | string | 是 | 单次运行标识，非空 |
| `eventType` | string | 是 | `RUN_STARTED` / `HEARTBEAT` / `SUCCEEDED` / `FAILED` / `CANCELLED` |
| `occurredAt` | number | 是 | 事件发生时间，epoch **毫秒整数**，>=0 |
| `scheduledStartAt` | number | 否 | 调度起始时间（epoch ms） |
| `slaDeadlineAt` | number | 否 | 本次运行的 SLA 截止点（epoch ms） |
| `attempt` | number | 否 | 第几次尝试，整数 >=1 |
| `message` | string | 否 | 附加信息（如失败原因） |

示例：

```json
{"protocolVersion":"1.0","pipelineId":"orders","runId":"r-001",
 "eventType":"RUN_STARTED","occurredAt":1727000000000,"attempt":1}
```

### 2.1 接入结论

成功时 HTTP 200，响应中 `outcome` 取值：

| outcome | 含义 |
| --- | --- |
| `ACCEPTED` | 事件被接受并推动状态（version +1） |
| `DUPLICATE_IGNORED` | 与历史上报签名完全相同（type\|occurredAt\|attempt\|message），幂等忽略，状态不变 |
| `LATE_IGNORED_TERMINAL` | 运行已终结，迟到事件被忽略，**终结态绝不回退** |

### 2.2 拒绝原因（不静默丢弃）

校验失败返回 `400`（JSON 无法解析）或 `422`（语义校验失败），响应体
`{"error":"EVENT_REJECTED","reason":..., "detail":...}`，`reason` 可区分：

| reason | 触发条件 | HTTP |
| --- | --- | --- |
| `MALFORMED_JSON` | 空报文 / 非 JSON / 根不是对象 | 400 |
| `MISSING_FIELD` | 必填字段缺失或为 null（含 protocolVersion 缺失） | 422 |
| `TYPE_MISMATCH` | 字段类型不符（如 occurredAt 传字符串、布尔、浮点） | 422 |
| `UNSUPPORTED_PROTOCOL_VERSION` | protocolVersion 存在但不是 `"1.0"` | 422 |
| `UNKNOWN_EVENT_TYPE` | eventType 不在五种枚举内 | 422 |
| `INVALID_FIELD_VALUE` | 空字符串、时间为负、attempt<1 等非法取值 | 422 |

所有拒绝事件记录在案，可通过 `GET /api/events/rejections?limit=50` 追溯（含原因、明细、原始载荷片段）。

---

## 3. 运行状态收敛规则

按 `runId` 维度，在 `ConcurrentHashMap#compute` **原子块**内完成，天然串行化同 run 的并发上报：

1. **重复**：相同事件签名已见过 → `DUPLICATE_IGNORED`，version 不变；
2. **终结保护**：run 已是 `SUCCEEDED/FAILED/CANCELLED` 时，任何新签名上报 →
   `LATE_IGNORED_TERMINAL`。先到的终态为准（先 FAILED 后迟到 SUCCEEDED 仍为 FAILED）；
3. **乱序**：运行中允许迟到事件补齐 `scheduledStartAt/slaDeadlineAt` 等字段，
   `lastEventAt/heartbeatAt` 单调取大；
4. **并发**：version 单调递增，每个唯一事件恰好被接受一次（`RunStateConvergenceTest` 16 线程
   ×100 事件、`AlertConcurrencyTest` 20 线程 ×50 协调均验证无丢失/无重复）。

管道维度聚合态（`PipelineRuntime`）同样以 `compute` 原子更新：lastSuccessAt/lastFailureAt
按事件时间单调取大；只有更新的 run 才能推进 currentRun，避免旧 run 迟到快照覆盖新 run。

健康结论优先级：

```
SLA_BREACHED > NEVER_SUCCEEDED > TRANSIENT_JITTER > SCHEDULE_LAGGING > STALE > HEALTHY
```

---

## 4. 健康判定与阈值（边界严格大于，不误报）

| 结论 | 判定依据 |
| --- | --- |
| `NEVER_SUCCEEDED` | 从未成功，且距首次观测 **严格大于** `neverSucceededGraceMs`；宽限内保持 HEALTHY |
| `STALE` | 有成功史、未配置调度周期，距上次成功 **严格大于** `freshnessThresholdMs` |
| `SCHEDULE_LAGGING` | 配置了周期：`now - (lastSuccessAt + scheduleIntervalMs)` **严格大于** `scheduleToleranceMs` |
| `SLA_BREACHED` | 当前运行中且满足任一：① 运行时长 **严格大于** `slaRunDurationMs`；② `now` **严格大于** 事件自带 `slaDeadlineAt` |
| `TRANSIENT_JITTER` | 当前 run 失败且失败后经过时间 **<=** `jitterGraceMs`；宽限内暂缓新鲜度/滞后告警（SLA 仍独立判定） |
| `HEALTHY` | 其余情形 |

边界处理（测试逐项覆盖）：

- 年龄/滞后/时长 **恰好等于阈值时一律不触发**（严格 `>`），时钟边界不误报；
- **刚恢复**：新 run 成功后计时从新的 lastSuccessAt 重新起算，立即回到 HEALTHY，不残留误报；
- 抖动宽限为**左闭右含**：失败后第 60000ms（默认）仍算抖动，超期后才恢复新鲜度判定；
- 每个 `HealthSnapshot` 都带 `reasons[]`，逐条记录实际值、阈值与边界处理结论，保证可解释。

---

## 5. 告警规则

告警 key = `pipelineId:alertType`，类型：`FRESHNESS`（含从未成功/STALE）、`SCHEDULE_LAG`、
`SLA_BREACH`。

| 生命周期动作 | 规则 |
| --- | --- |
| **触发** | 每轮健康评估后，对 firing 类型：无记录（或已 RECOVERED）→ 建告警并发送 **一次** `TRIGGERED`（WARNING） |
| **去重合并** | 已存在且未恢复：仅 `count+1` 与更新 lastTriggeredAt，**不再发通知**；并发协调在同一把锁内读改写，计数不丢失 |
| **升级** | ACTIVE 且持续时间 **严格大于** `escalationAfterMs` → 升级 `ESCALATED/CRITICAL`，发一次 `ESCALATED`，每告警仅升级一次 |
| **抑制** | 评估时刻处于维护窗口：首次触发记为 `SUPPRESSED`（`suppressed=true`），不发通知，但记录与计数完整保留可追溯；窗口结束问题仍在则补发一次 `TRIGGERED` |
| **恢复** | 本轮不再 firing 的活跃（含抑制）告警 → 置 `RECOVERED` 并发一次 `RECOVERED`；恢复后不在默认告警列表中残留；再次触发视为新一轮重新通知 |

通知流水（`TRIGGERED/ESCALATED/RECOVERED`）可通过接口查询，便于核验“不重复通知”。

---

## 6. 可配置项（`application.properties` / 环境变量覆盖）

前缀 `pipeline.observability`：

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `evaluation-interval-ms` | 5000 | 周期评估间隔 |
| `scheduler-enabled` | true | 是否启用内置调度（测试置 false 手动 tick） |
| `defaults.never-succeeded-grace-ms` | 60000 | 首次观测后“从未成功”宽限 |
| `defaults.freshness-threshold-ms` | 300000 | 新鲜度超期阈值（未配调度时生效） |
| `defaults.schedule-interval-ms` | 0（禁用） | 调度周期，>0 启用调度滞后判定 |
| `defaults.schedule-tolerance-ms` | 30000 | 调度容忍/抖动窗口 |
| `defaults.sla-run-duration-ms` | 0（禁用） | 最大运行时长 SLA |
| `defaults.jitter-grace-ms` | 60000 | 失败后短暂抖动宽限 |
| `defaults.escalation-after-ms` | 900000 | 告警 WARNING→CRITICAL 升级时长 |

**按管道覆盖**（仅需给出覆盖项，其余沿用默认），例如：

```properties
pipeline.observability.pipelines.scheduled.schedule-interval-ms=60000
pipeline.observability.pipelines.scheduled.schedule-tolerance-ms=30000
pipeline.observability.pipelines.critical.freshness-threshold-ms=60000
```

---

## 7. HTTP 接口一览

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/events` | 上报运行事件 |
| `GET  /api/events/rejections` | 被拒绝事件留痕（原因可区分） |
| `GET  /api/pipelines` / `/{id}` | 管道聚合态列表/详情 |
| `GET  /api/pipelines/{id}/runs` | 管道下各 run 收敛状态 |
| `GET  /api/runs/{runId}` | 单个 run 状态 |
| `GET  /api/health` / `/{pipelineId}` | 实时评估并返回健康快照（含 reasons 判定依据） |
| `POST /api/health/tick` | 手动驱动一轮评估与告警协调 |
| `GET  /api/alerts?includeRecovered=true` | 告警列表（默认不含已恢复） |
| `GET  /api/alerts/notifications` | 触发/升级/恢复通知流水 |
| `GET/POST/DELETE /api/maintenance/windows[/{id}]` | 维护窗口管理 |

维护窗口请求体：`{"pipelineId":null,"startAt":1727000000000,"endAt":1727000300000,"reason":"deploy"}`
（`pipelineId` 为 null 表示全局窗口，窗口区间左闭右开）。

---

## 8. 验证方法

```bash
mvn test
```

测试清单（关键输入与判定依据均以 SLF4J 打印到测试日志）：

| 测试类 | 覆盖内容 |
| --- | --- |
| `EventValidatorTest` | 六类拒绝原因可区分、合法事件解析 |
| `RunStateConvergenceTest` | 乱序终态不回退、重复幂等、先到终态优先、16 线程并发收敛计数 |
| `HealthEvaluatorTest` | 从未成功宽限、新鲜度严格边界、调度滞后边界、刚恢复不误报、抖动抑制、运行 SLA 与事件截止点边界 |
| `AlertServiceTest` | 触发去重合并、边界升级仅一次、恢复不残留不重复、维护窗口抑制留痕与窗口结束补发 |
| `AlertConcurrencyTest` | 20 线程并发协调，合并计数无丢失、TRIGGERED 恰好一条 |
| `EndToEndIntegrationTest` | MockMvc 全链路：HTTP 拒绝留痕、乱序/重复/迟到、新鲜度与 SLA/调度边界、告警去重恢复、8 线程并发接入 |

手动联表示例：

```bash
curl -s -XPOST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"protocolVersion":"1.0","pipelineId":"orders","runId":"r1","eventType":"RUN_STARTED","occurredAt":1000}'
curl -s -XPOST localhost:8080/api/health/tick | jq .
curl -s 'localhost:8080/api/alerts?includeRecovered=true' | jq .
```
