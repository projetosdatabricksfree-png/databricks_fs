# Ingestion Decision Matrix (CTAS × COPY INTO × Auto Loader)

Quick reference for choosing an ingestion method on Databricks. Grounded in the
exam blueprint (Section 2) and ADR-001 of the BCB Lakehouse Lab.

## Comparison

| Dimension | CTAS / `read_files` | COPY INTO | Auto Loader |
|-----------|---------------------|-----------|-------------|
| Processing model | Batch, full | Incremental (SQL) | Incremental (stream/checkpoint) |
| File tracking | None (reprocesses all) | Tracks loaded files (idempotent) | RocksDB checkpoint |
| Scales to millions of files | ❌ degrades | ⚠️ degrades | ✅ efficient discovery |
| Schema inference | ✅ | limited | ✅ |
| Schema evolution | ❌ | ❌ | ✅ `addNewColumns` / `rescue` |
| Rescued data column | ❌ | ❌ | ✅ `_rescued_data` |
| Inside SDP | via `read_files` | not typical | ✅ `STREAM read_files` |
| Typical use | One-off rebuild, exploration | Seed a small dimension | Bronze standard |

## Rules of thumb

1. **Default to Auto Loader** for any source that produces files over time.
2. **COPY INTO** for a simple, occasional reference/seed load where idempotency via
   SQL is enough and volume is small.
3. **CTAS / `read_files`** only for one-shot full rebuilds or ad-hoc analysis.

## Schema evolution behaviors (Auto Loader)

- `addNewColumns` (default): stream **fails** on first new column, then succeeds after
  restart with the column added. Rely on job retries.
- `rescue`: unexpected data captured in `_rescued_data` without failing the stream.
- `none` / `failOnNewColumns`: reject changes (strict contracts).

## Idempotency checks (exam-relevant)

- **COPY INTO** rerun on the same files → **0 rows** inserted (files already tracked).
- **Auto Loader `availableNow`** rerun → does **not** reprocess backlog (checkpoint).
- Forge a `_rescued_data` case by adding an unexpected field to a landing JSON.

## Related

- ADR-001 (ingestion strategy) · ADR-002 (dedup/CDC) · `01_ingestao_comparativo.py`.
