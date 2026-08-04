# DeltaFlow

DeltaFlow 是一个轻量的增量同步核心，用于把交易库中持续变化的 `orders`
同步到分析库。首次运行做全量回填，之后只读取新增或更新订单；中途停止后从
已提交 checkpoint 继续，重试不会制造重复订单。

> 集成接缝：`tests/test_e2e.py` 和 `scripts/benchmark.py` 依赖公共 API
> `qsync.connectors.sqlite_source.SQLiteSource` 与
> `qsync.connectors.sqlite_sink.SQLiteSink`。当 checkout 尚未包含连接器时，测试会
> 明确 skip，benchmark 会返回缺失 API。现有 `qsync` CLI 仍保留
> JSONL→SQLite 的早期兼容入口，不把它冒充为 order connector CLI。

## 真实场景

运营库接收下单、支付、发货和退款状态变化；分析库为仪表盘提供近实时数据。
一条订单最少包含：

```sql
CREATE TABLE orders (
  id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  status TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX idx_orders_updated_at_id ON orders(updated_at, id);
```

每次调度读取 checkpoint 之后的页，目标端按 `id` upsert，并在同一事务内保存
数据和 checkpoint。这是“至少一次读取 + 幂等落库”得到的 effectively-once
结果，不声称分布式 exactly-once。

## 架构

```text
SQLite orders source
  └─ SQLiteSource.read_batch(cursor, batch_size)
       └─ keyset page: ORDER BY updated_at, id
            └─ run_sync (connector-neutral orchestration)
                 └─ SQLiteSink.commit_batch(records, next_cursor)
                      ├─ idempotent order upsert
                      └─ atomic checkpoint update
```

核心只依赖 `Source` / `Sink` protocol，不知道 SQL 表或订单字段。SQLite 是参考连接器；
替换为 PostgreSQL、MySQL 或 API 时，保持以下契约即可：

- Source 按 `(updated_at, id)` 严格升序返回游标之后的记录。
- 非空批次必须返回一个向前推进的 `next_cursor`。
- Sink 必须原子提交记录和 checkpoint。
- 目标端不允许较旧 `updated_at` 覆盖较新版本。

不能只用 `updated_at` 作游标：同一毫秒可能有多笔订单。DeltaFlow 使用
`(updated_at, id)` 作稳定复合游标：

```sql
WHERE (updated_at, id) > (:updated_at, :id)
ORDER BY updated_at, id
LIMIT :batch_size
```

Demo 和 benchmark 生成的 source schema 会创建该复合索引，使增量页能从
checkpoint 边界做索引搜索，并直接按页面所需顺序返回。Connector 不会为
任意 source 数据库自动创建或修改索引；生产 source 应由 schema migration
显式管理该索引。

## 60 秒 demo

当 checkout 已包含 SQLite connectors 时，以下命令会生成真实 SQLite `orders`
数据库，依次测量首次全量、
新增 1 万笔后的增量和 no-op，最后输出一份 JSON：

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/python scripts/benchmark.py \
  --rows 100000 --incremental-rows 10000 --batch-size 5000
```

想保留源库和目标库以便手工检查：

```bash
.venv/bin/python scripts/benchmark.py --workspace ./benchmark-output
sqlite3 benchmark-output/analytics.db \
  'select status, count(*) from orders group by status'
```

`--workspace` 不会覆盖已有的 `commerce.db` 或 `analytics.db`，适合在 shell/CI 中
重复调用。不传时使用临时目录并自动清理。

## 验收与 benchmark

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 scripts/benchmark.py --rows 1000000 --batch-size 10000
```

E2E 验收覆盖：

- 首次全量回填；
- 新增与更新的增量同步；
- no-op 不读不写；
- `max_batches` 中断后从已提交批次续跑；
- 跨批次的相同 timestamp 不漏数；
- 旧版本不覆盖目标端新版本。

在 SQLite connector 缺席时，`test_e2e.py` 会以明确原因 skip，而不引入一份伪连接器
让结果假绿。benchmark 会以 exit code 2 失败并列出缺少的预期 API。

## 边界与后续扩展

当前边界：

- 单次运行是一个 source 到一个 sink；并发调度和分片不在 MVP 内。
- 只同步 insert/update；物理删除需要 tombstone 或 CDC 事件。
- `updated_at` 要求规范化为 UTC ISO-8601，且文本顺序等于时间顺序。
- 运行期间新产生一条“timestamp 早于已提交游标”的源记录无法被 keyset
  扫描发现；需上游单调时钟、lookback 窗口或 CDC。
- 核心不包含 schema mapping、DLQ、密钥管理、分布式锁和指标导出。

后续可在不改动 pipeline 的情况下增加 PostgreSQL/MySQL connectors、字段映射、
删除事件、Prometheus/OpenTelemetry 指标、失败批次 DLQ，以及面向多数据集的调度层。

更完整的一致性推导见 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)。
