# qsync — 高性能增量数据同步 CLI

一个两小时 Vibe Coding 挑战的可运行 MVP：将持续追加的 JSONL 数据增量同步到
SQLite，支持断点续传、批量写入、幂等 upsert、并行解析和性能指标。

## 为什么这样设计

- **字节游标，而非重复扫描**：每个源文件记录已提交的 byte offset，下一次直接 seek。
- **数据与游标原子提交**：目标数据和 checkpoint 在同一个 SQLite 事务中更新，崩溃后
  不会出现“游标前进但数据没写入”的丢数窗口。
- **幂等且拒绝旧版本覆盖**：以 `id` 为主键 upsert，仅当 `updated_at` 不早于目标记录时更新。
- **吞吐优先**：按批读取、`executemany` 写入、WAL、`synchronous=NORMAL`；可选并行 JSON 解析。
- **处理正在写入的文件**：末尾没有换行的半条记录不消费，等下次补完整再同步。

该 MVP 的边界是单机、append-only JSONL。生产版可把 Source/Sink 抽象扩展到 MySQL、
PostgreSQL、Kafka 或对象存储，并加入 schema mapping、DLQ、指标上报和 CDC 删除事件。

## 记录格式

每行一个 JSON object，必须有 `id` 和可按字典序比较的 ISO-8601 `updated_at`：

```json
{"id":"user-1","updated_at":"2026-08-04T10:00:00Z","name":"Alice"}
```

## 60 秒演示

无需第三方运行时依赖：

```bash
python3 -m venv .venv
.venv/bin/pip install -e .

# 生成 10 万条并首次同步
.venv/bin/qsync generate demo.jsonl --count 100000
.venv/bin/qsync sync demo.jsonl demo.db --batch-size 5000

# 追加 1 万条，只读取和写入新增部分
.venv/bin/qsync generate demo.jsonl --count 10000 --append
.venv/bin/qsync sync demo.jsonl demo.db --batch-size 5000

# 再跑一次是 no-op；查看 checkpoint
.venv/bin/qsync sync demo.jsonl demo.db
.venv/bin/qsync status demo.db
```

每次命令输出 JSON，包括 `rows_read`、`rows_written`、`elapsed_seconds` 和
`rows_per_second`，便于脚本消费和现场展示。

## CLI

```text
qsync generate <file> [--count N] [--append]
qsync sync <source.jsonl> <target.db> [--batch-size N] [--workers N] [--reset]
qsync status <target.db>
```

`--reset` 只重置该源的读取游标并重新 upsert，不清空目标表。若源文件被截断，工具默认
拒绝继续，以避免静默错读；确认后可显式使用 `--reset`。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

覆盖首次/增量/no-op、批次中断恢复、半条记录、版本冲突及源文件截断保护。

## 两小时实现取舍

1. 0–20 分钟：约束、CLI 和一致性模型。
2. 20–70 分钟：批处理引擎、事务 checkpoint、幂等写入。
3. 70–100 分钟：异常路径与测试。
4. 100–120 分钟：benchmark、文档和演示。

