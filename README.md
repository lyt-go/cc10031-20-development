# 工业传感器时序采集与降采样平台

面向工业传感器高频时序数据的测点接入、边缘缓冲上送、单位标定、时间对齐、降采样归并与分层归档的采集平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 测点台账 | `sensor_point` | 测点档案 | 测点编号、所属产线、传感器型号 |
| 边缘缓冲 | `edge_buffer` | 缓冲分片 | 分片编号、测点编号、起始时刻 |
| 批量上送 | `upload_batch` | 上送批次 | 批次编号、分片范围、测点数量 |
| 单位标定 | `unit_calibrate` | 标定记录 | 标定编号、测点编号、原始单位 |
| 时间对齐 | `time_align` | 对齐规则 | 规则编号、对齐粒度、时区基准 |
| 断点填补 | `gap_fill` | 填补区间 | 区间编号、测点编号、缺失起点 |
| 尖峰剔除 | `spike_filter` | 异常采样 | 异常编号、测点编号、采样时刻 |
| 降采样 | `downsample` | 降采样任务 | 任务编号、源序列、源采样频率 |
| 窗口归并 | `rollup_window` | 归并窗口 | 窗口编号、测点编号、窗口类型 |
| 序列索引 | `series_index` | 索引分区 | 索引编号、测点编号、时间分区 |
| 冷热分层 | `tier_storage` | 分层策略 | 策略编号、时间边界、热区范围 |
| 采集心跳 | `device_health` | 心跳记录 | 心跳编号、采集终端、上报时刻 |
| 接入水位 | `ingest_watermark` | 水位标记 | 水位编号、测点编号、事件时间水位 |
| 区间重采 | `range_replay` | 重采任务 | 重采编号、测点编号、起始时刻 |
| 采集血缘 | `series_lineage` | 血缘链路 | 链路编号、测点编号、上送批次 |
| 序列导出 | `series_export` | 导出任务 | 导出编号、测点范围、时间范围 |
| 时序质量 | `series_quality` | 质量报告 | 报告编号、测点编号、统计窗口 |
| 序列快照 | `series_snapshot` | 版本快照 | 快照编号、口径说明、测点范围 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 接入监控：缺口定位与重传结算

接入监控不与通用资源路由混用，固定路径在 `app/routers/monitor.py` 注册，领域逻辑在
`app/services/ingest_monitor.py`。它按「测点 + 事件时间 + 到达顺序」登记服务端实收凭证，
重复上送只追加凭证流水、绝不把同一采样重复计入实收集合；相邻与交叠的已收批次分别对账。

| 方法与路径 | 作用 |
| --- | --- |
| `POST /api/ingest-monitor/receipts` | 登记一批实际到达样本（接收凭证），按批次与到达顺序留痕，重复到达不重复计点 |
| `GET /api/ingest-monitor/gaps?point=&start_time=&end_time=` | 按事件时间窗口对账实收样本，定位剩余缺口（可重传区间），固化查询快照并返回各批次对账 |
| `GET /api/ingest-monitor/snapshots/{snapshot_id}` | 读取某次缺口查询的捕获快照，作为派发与结算基线 |
| `POST /api/ingest-monitor/replays` | 依据快照发起重传；派发前重新对账，剔除查询后迟到补齐的部分，返回实际送出与未发送区间 |
| `POST /api/ingest-monitor/replays/{replay_key}/settle` | 重传落地后按实际实收结算；半段补齐只更新剩余缺口，不因发送成功关闭整段 |

关键语义：

- 缺口查询只回答“查询时刻”的实收缺口，并把期望/实收/缺口序号与批次对账写入
  `data/gap_reconcile/<snapshot_id>.json`，作为后续派发的合理捕获范围。
- 派发时重新读取接收凭证：查询与派发之间的迟到上送会从待发范围剔除（`late_covered_after_query`），
  续发只覆盖快照缺口，新到数据不会被当成缺口重发；快照缺口之外的重传请求直接拒绝。
- 结算以凭证为准：发了但没落地的采样不能划掉缺口；重复到达的采样只计一次
  （`duplicate_in_replay_count` 单独留痕）；`fully_closed=false` 时区间重采申请保持待重采，
  剩余缺口可再次发起重传。

数据产物：`data/receipts/<测点>_receipts.jsonl`（凭证流水）、
`data/gap_reconcile/`（查询快照）、`data/replay_settlements/`（重传结算与审计事件）。
验收用例见 `backend/tests/test_ingest_monitor.py`。

领域：数据工程
子方向：数据采集、爬虫与RPA
