# 本地设备遥测接收 API（FastAPI + SQLite）

按设备维护**序列号滑动窗口**的本地遥测收集服务。设备批次带递增序列号且可能乱序到达，服务负责：

- **幂等去重**：重复序列号（跨批次或同批次）直接丢弃，不重复投递；
- **缺口暂存**：窗口内先到的“未来”序列号进入暂存区，缺口以 `OPEN` 标记等待；
- **超时断档**：前沿缺口等待超过可配置时间后提升为 `MISSING`（断档），前沿跨过断档继续推进；
- **断档重放**：提供重放接口回补已判定丢失的遥测，且重放本身幂等；
- **并发一致**：同一设备写入按设备串行化并包裹在单条 SQLite 事务内，任意并发批/重放只产生一次投递。

## 启动后信息（默认值）

| 项目 | 值 |
| --- | --- |
| API 基础地址 | `http://localhost:8080` |
| 健康检查 | `GET http://localhost:8080/health` |
| 交互式文档 | `http://localhost:8080/docs`（Swagger UI） |
| OpenAPI | `http://localhost:8080/openapi.json` |
| 监听端口 | `8080`（容器内与宿主机均为 8080） |
| 数据库 | SQLite，容器内 `/data/telemetry.db`（Docker 命名卷持久化，WAL 模式） |

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build -d
docker compose logs -f telemetry-api
curl http://localhost:8080/health
```

停止与清理：

```bash
docker compose down            # 停止，保留数据卷
docker compose down -v         # 停止并删除遥测数据
```

### 本地直接运行（不用 Docker）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export TELEMETRY_DATABASE_PATH=./telemetry.db   # 默认是 /data/telemetry.db
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

### 运行测试

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

测试包含：乱序补齐、跨批次/同批次幂等、超时断档、断档重放、滑动窗口前后沿拒绝、
20 路并发批次精确一次投递，以及使用**真实后台扫描器 + 真实时钟**的端到端断档用例。

## 配置（环境变量，前缀 `TELEMETRY_`）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `TELEMETRY_DATABASE_PATH` | `/data/telemetry.db` | SQLite 文件路径；`:memory:` 仅用于测试 |
| `TELEMETRY_HOST` | `0.0.0.0` | 监听地址 |
| `TELEMETRY_PORT` | `8080` | 监听端口 |
| `TELEMETRY_INITIAL_SEQ` | `0` | 设备首次出现时的起始期望序列号 |
| `TELEMETRY_WINDOW_SIZE` | `1000` | 滑动窗口跨度（见下） |
| `TELEMETRY_WAIT_TIMEOUT_MS` | `30000` | 前沿缺口等待多久后判定断档（毫秒） |
| `TELEMETRY_SWEEP_INTERVAL_MS` | `1000` | 后台扫描器检查到期缺口的周期（毫秒） |
| `TELEMETRY_ENABLE_SWEEPER` | `true` | 是否启用后台扫描器（关闭时缺口在下次请求时惰性到期） |

设备级参数还可在**每个设备的第一个批次**里按设备覆盖：`initial_seq`、`window_size`、
`wait_timeout_ms`（仅首次注册生效，后续批次中的值被忽略，保证配置不可被请求篡改）。

## API

基础前缀：`/api/v1`。所有请求/响应均为 JSON。

### 1. 提交批次

```
POST /api/v1/devices/{device_id}/batches
202 Accepted
```

请求体：

```json
{
  "initial_seq": 0,
  "window_size": 1000,
  "wait_timeout_ms": 30000,
  "entries": [
    {"seq": 12, "ts": 1726000000.12, "payload": {"temp": 42.1}},
    {"seq": 10, "payload": "任意 JSON"}
  ]
}
```

- `entries` 必填、非空、最多 10000 条；`seq` 为非负整数；`payload` 为任意 JSON。
- 单条结果状态（响应中的每个 entry）：

| 状态 | 含义 |
| --- | --- |
| `accepted` | 新序列号，已入库（若补齐前沿则同批次内 `delivered=true`）；窗口内迟到的原报也会自动补掉 OPEN 缺口或刚超时的 MISSING 断档 |
| `duplicate` | 已投递或在同批次内重复，幂等丢弃 |
| `already_pending` | 已在暂存区等待，幂等丢弃 |
| `out_of_window` | 落在窗口之外：过早（断档已滑出窗口下限，无法再排序）或过远超前（缺口会溢出窗口） |

响应汇总：`accepted`、`filled`（accepted 中本次已投递的数量）、各丢弃原因计数、
本次实际按序投递的 `delivered_seq` 列表、逐条 `results`，以及提交后的 `window` 状态。

### 2. 查询窗口状态

```
GET /api/v1/devices/{device_id}/window
```

```json
{
  "device_id": "sensor-a",
  "contiguous_next": 47,
  "window_size": 1000,
  "wait_timeout_ms": 30000,
  "window_low_watermark": -953,
  "pending_count": 3,
  "pending_min": 49,
  "pending_max": 51,
  "open_gap_count": 2,
  "missing_gap_count": 0,
  "open_gaps": [47, 48],
  "next_gap_expires_in_ms": 12340
}
```

未知设备返回 `404`。读取会顺带应用到期判定，因此状态始终是最新的。

### 3. 缺口 / 断档列表

```
GET /api/v1/devices/{device_id}/gaps[?status=OPEN|MISSING]
```

返回该设备全部 `OPEN`（暂存中的缺口）与 `MISSING`（已超时断档）位置，
含 `opened_at`、`finalized_at`，对 OPEN 项给出 `expires_in_ms`。

### 4. 断档重放

```
POST /api/v1/devices/{device_id}/gaps/replay
```

```json
{"entries": [{"seq": 47, "payload": {"temp": 41.9, "replayed": true}}, {"seq": 48}]}
```

用于回补**已判定为 MISSING** 的遥测。逐条结果：

| 状态 | 含义 |
| --- | --- |
| `filled` | 成功回补一个断档 |
| `duplicate` | 该序列号已投递（重复重放，幂等无副作用） |
| `already_pending` | 该序列号已在暂存区，走正常提交即可 |
| `already_gap` | 缺口仍为 OPEN（还没超时），应走正常提交 |
| `not_a_gap` | 从未被声明为缺口、已滑出窗口下限、或位于前沿之后（未来序列号） |

回补后前沿会立即级联推进（跨过已修补的断档、投递后方暂存链）。

### 系统接口

- `GET /health` → `{"status":"ok","database":...,"sweeper_running":...}`
- `GET /docs` Swagger UI；`GET /openapi.json`。

## 窗口与断档语义

```
          <---- window_size ---->|             |<---- window_size ---->
  已投递/已断档   低水位        contiguous_next                     高水位(不含)
seq  <  frontier-ws  拒绝        (前沿, 第一个未知位置)     seq >= frontier+ws 拒绝
```

- **前沿 `contiguous_next`**：严格小于它的所有序列号都已按序投递或已声明断档。
- 序列号落在半开区间 `[contiguous_next - window_size, contiguous_next + window_size)`
  之外的新数据返回 `out_of_window`。
- 前沿缺口（或其后最早暂存项的到达）起计时，等待 `wait_timeout_ms`：
  - 期间数据补齐 → OPEN 标记删除，数据级联投递，不产生断档；
  - 超时 → OPEN 提升为 MISSING，前沿跨过断档，断档后的暂存数据继续投递。
- 后台扫描器每 `sweep_interval_ms` 兜底推进；即使关闭扫描器，下一次读写请求也会惰性到期。
- 断档标记保留，可查询、可重放。窗口内迟到的原始遥测会自动回补（无论缺口是 OPEN 还是刚超时的 MISSING）；重放接口用于设备按断档清单**显式重传**。断档一旦滑出窗口下限，则只能通过重放接口在其仍有效时处理，过旧的迟到数据返回 `out_of_window`。

## 并发一致性

- 每个设备一把 `asyncio.Lock`：同设备的提交 / 查询 / 重放 / 扫描器互斥，不同设备完全并行。
- 每次状态变更在单条 `BEGIN IMMEDIATE` 事务内完成（去重判定 → 写入 → 前沿推进 → 快照），
  配合 SQLite WAL 与 `busy_timeout`，不存在“查到未提交中间态”。
- 去重在写事务内基于主键判定（`(device_id, seq)`），重复批次只会有一份数据，
  投递序列中每个序列号恰好出现一次（见并发测试 `test_concurrent_batches_deliver_each_seq_once`）。
- 单 uvicorn worker + 单 SQLite 文件的部署形态适合本地/边缘收集；扩展到多进程需要把
  设备锁换成跨进程协调，不在本地版范围内。

## 数据模型（SQLite）

- `devices`：每设备前沿、初始序列号、窗口、等待超时；
- `pending_entries`：窗口内等待的乱序遥测；
- `delivered_entries`：已按序投递的遥测（保留用于幂等判定与重放审计）；
- `gaps`：`OPEN` / `MISSING` 缺口标记及发现/终结时间。

## 目录结构

```
app/
  config.py    # 环境变量配置（pydantic-settings）
  db.py        # SQLite 连接、WAL、schema、事务重试
  schemas.py   # 请求/响应模型
  service.py   # 滑动窗口状态机（去重/暂存/断档/重放 + 每设备锁）
  sweeper.py   # 后台缺口到期扫描器
  main.py      # FastAPI 路由与生命周期
tests/         # pytest（功能、并发、真实时钟端到端）
Dockerfile
docker-compose.yml
requirements.txt / requirements-dev.txt
```
