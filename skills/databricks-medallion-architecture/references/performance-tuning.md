# Performance Tuning for Medallion Architecture

## Query Performance Optimization

### Z-Order Clustering for Common Queries

Z-order on join keys dramatically improves query performance by co-locating related data:

```sql
-- Silver layer: Optimize for customer_id joins
OPTIMIZE silver.customers_unified
ZORDER BY (customer_id);

-- Gold layer: Optimize for time-based analytics
OPTIMIZE gold.daily_revenue_by_product
ZORDER BY (date, product_category, region);

-- Fact table: Optimize for common dimension joins
OPTIMIZE gold.fact_orders
ZORDER BY (customer_id, product_id, date);
```

**Benefits:**
- 10-100x faster queries on clustered columns
- Reduced data shuffle during joins
- Better cache utilization
- Trade-off: 5-10% slower write performance

**When to use Z-order:**
- Columns in WHERE clauses (date, region, customer_id)
- Foreign key columns (dimension joins)
- High-cardinality columns that filter > 50% of data

**When NOT to use Z-order:**
- Low-cardinality columns (is_active, gender)
- Columns rarely used in filters
- Tables < 1GB (Z-order overhead exceeds benefits)

---

### Partitioning Strategy

Effective partitioning reduces data scans:

```sql
-- Bronze: Partition by ingestion date (retention policy)
CREATE TABLE bronze.raw_events (
  _ingestion_timestamp TIMESTAMP,
  _partition_year INT,
  _partition_month INT,
  _partition_day INT,
  -- ... columns ...
)
PARTITIONED BY (_partition_year, _partition_month, _partition_day);

-- Silver: Partition by quarter (balance between query pruning and file count)
CREATE TABLE silver.transactions (
  transaction_date DATE,
  year INT,
  quarter INT,
  -- ... columns ...
)
PARTITIONED BY (year, quarter);

-- Gold: Partition by year only (data already aggregated)
CREATE TABLE gold.monthly_revenue (
  month DATE,
  year INT,
  -- ... columns ...
)
PARTITIONED BY (year);

-- Query with partition pruning
SELECT * FROM gold.monthly_revenue
WHERE year = 2024  -- Partition pruned: scans only 2024 partitions
  AND month >= '2024-01-01';
```

**Partition sizing:**
- **Too many partitions** → Metadata overhead; slow jobs
- **Too few partitions** → Large files; slow queries
- **Target**: 10-1000 partitions per table, ~100MB per partition

---

### Compaction & Small File Problem

Delta Lake accumulates small files. Optimize them:

```sql
-- Manual compaction (combines small files)
OPTIMIZE silver.customers_unified;

-- Compaction with Z-order clustering
OPTIMIZE silver.customers_unified
ZORDER BY (customer_id);

-- Auto-compaction (runs after writes)
ALTER TABLE silver.customers_unified
SET TBLPROPERTIES (
  'delta.autoCompact' = 'true',
  'delta.targetFileSize' = '100mb'
);

-- Monitor file count
DESCRIBE DETAIL silver.customers_unified;
-- Check "numOfFiles" — should be < 100 for < 10GB table

-- Schedule weekly compaction
-- (Use Databricks Jobs or Airflow)
OPTIMIZE silver.customers_unified ZORDER BY (customer_id);
```

**Impact:**
- ✅ Reduces file count → faster queries
- ✅ Improves cache hit rate
- ❌ Read/write latency during OPTIMIZE (run off-peak)

---

## Query Design Patterns

### Pattern 1: Incremental Processing (Avoid Full Scans)

```sql
-- ❌ BAD: Full table scan
SELECT * FROM silver.transactions
WHERE amount > 1000;

-- ✅ GOOD: Partition + date filter
SELECT * FROM silver.transactions
WHERE year = 2024
  AND month = MONTH(CURRENT_DATE())
  AND amount > 1000;
```

### Pattern 2: Star Schema Joins (Pre-Join in Gold)

```sql
-- ❌ BAD: Join on every query (slow; redundant computation)
SELECT
  f.order_id,
  d.customer_name,
  d.city,
  f.amount
FROM fact_orders f
JOIN dim_customer d ON f.customer_id = d.customer_id
WHERE f.date >= CURRENT_DATE() - 30;

-- ✅ GOOD: Pre-joined in gold layer
SELECT * FROM gold.orders_with_customer
WHERE date >= CURRENT_DATE() - 30;

-- Gold table definition (denormalized)
CREATE TABLE gold.orders_with_customer AS
SELECT
  f.order_id,
  f.customer_id,
  f.product_id,
  f.amount,
  d.customer_name,
  d.city,
  d.country,
  f.date
FROM fact_orders f
JOIN dim_customer d ON f.customer_id = d.customer_id;

-- Refresh daily
INSERT OVERWRITE gold.orders_with_customer
SELECT ... FROM fact_orders f JOIN dim_customer d ...;
```

### Pattern 3: Materialized Views for Complex Aggregations

```sql
-- Refresh materialized view daily
CREATE OR REPLACE VIEW gold.daily_revenue_summary AS
SELECT
  date,
  region,
  product_category,
  COUNT(DISTINCT order_id) as order_count,
  SUM(amount) as total_revenue,
  AVG(amount) as avg_order_value
FROM gold.fact_orders
WHERE date >= CURRENT_DATE() - 365
GROUP BY date, region, product_category;

-- Refresh job (run nightly)
REFRESH MATERIALIZED VIEW gold.daily_revenue_summary;
```

---

## Compute Performance

### Shuffle Operations (Most Expensive)

Shuffles redistribute data across worker nodes — minimize them:

```sql
-- ❌ BAD: Multiple shuffles (groupby → join → groupby)
SELECT
  t.region,
  c.segment,
  COUNT(*) as orders
FROM transactions t
GROUP BY t.region  -- Shuffle #1
JOIN customers c ON t.customer_id = c.customer_id  -- Shuffle #2
GROUP BY t.region, c.segment;  -- Shuffle #3

-- ✅ GOOD: Single pass with pre-joined table
SELECT
  region,
  customer_segment,
  COUNT(*) as orders
FROM gold.transactions_with_customer  -- Pre-joined in gold
GROUP BY region, customer_segment;
```

### Broadcast Joins for Dimensions

```python
# Python/PySpark: Broadcast small dimension tables
from pyspark.sql.functions import broadcast

dim_customer_broadcast = spark.read.table("gold.dim_customer")  # < 1GB

result = fact_orders.join(
  broadcast(dim_customer_broadcast),
  on="customer_id"
)
# Broadcast avoids shuffle; fast parallel joins
```

### Adaptive Query Execution (AQE)

Enable to auto-optimize shuffles:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# AQE will:
# - Coalesce small partitions
# - Detect skew; use salting if needed
# - Reduce shuffle partitions based on actual data
```

---

## Caching Strategies

### Delta Cache (Recommended)

```sql
-- Enable Delta cache for hot tables
ALTER TABLE silver.customers_unified
SET TBLPROPERTIES ('delta.cacheMinFileSize' = '1b');

-- Cache will automatically store in SSD/memory
-- Reset cache if data changes
REFRESH TABLE silver.customers_unified;
```

### RDD Caching (PySpark)

```python
# Cache in-memory for repeated queries
df = spark.read.table("silver.transactions")
df.cache()

# Run multiple queries on cached DF
result1 = df.filter(col("amount") > 100).count()
result2 = df.filter(col("region") == "US").count()

df.unpersist()  # Clear cache
```

### Query Result Caching

```sql
-- Enable query-level caching (Photon enabled clusters only)
SET spark.databricks.query.cache.enabled = true;

-- Same query runs 10-100x faster (sub-second cache hit)
SELECT * FROM gold.revenue_summary WHERE date = CURRENT_DATE();
SELECT * FROM gold.revenue_summary WHERE date = CURRENT_DATE();  -- Cache hit
```

---

## Cluster Configuration for Medallion Workloads

| Workload | Cluster Type | Driver | Workers | Auto-Scaling |
|----------|---|---|---|---|
| **Real-time ingestion** (Bronze) | All-purpose | 4 vCPU | 8-16x 4 vCPU | Yes (0-32 workers) |
| **Batch transformations** (Silver) | Jobs | 4 vCPU | 16-64x 8 vCPU | Yes (0-64 workers) |
| **Analytics queries** (Gold) | SQL Endpoint | 8 vCPU | 16-128x 16 vCPU | Yes (1-128 workers) |

**Recommendations:**
- **Medallion ingestion**: Always-on cluster with auto-scaling
- **Transformation pipelines**: Jobs cluster (cheaper; spin up for jobs)
- **BI/analytics**: Serverless SQL Endpoints (no infrastructure management)

---

## Monitoring & Debugging Slow Queries

### Query Metrics

```sql
-- Check execution time by stage
EXPLAIN EXTENDED
SELECT * FROM gold.fact_orders f
JOIN gold.dim_customer c ON f.customer_id = c.customer_id
WHERE f.date >= CURRENT_DATE() - 30;

-- Look for:
-- - Scan time (partition pruning effective?)
-- - Shuffle time (can we reduce?)
-- - Join type (broadcast vs. sort merge?)
```

### Databricks SQL Dashboard

```python
# Use Databricks SQL Dashboard to monitor:
- Query execution time (cluster compute time)
- Shuffle bytes (data movement cost)
- Number of rows (cardinality estimates vs. actual)
- Plan optimality (is AQE improving it?)
```

### Delta Statistics

```sql
-- Collect statistics for better query planning
ANALYZE TABLE silver.customers_unified
COMPUTE STATISTICS FOR COLUMNS (customer_id, email);

-- Check statistics
DESCRIBE TABLE EXTENDED silver.customers_unified;
```

---

## Common Performance Issues & Fixes

| Issue | Symptom | Diagnosis | Fix |
|-------|---------|-----------|-----|
| **High shuffle** | Job slow; high network I/O | EXPLAIN shows many shuffles | Pre-join in gold; broadcast small tables |
| **Small file problem** | Many small files; slow listing | `DESCRIBE DETAIL` shows 1000+ files | `OPTIMIZE` table; enable auto-compact |
| **No partition pruning** | Full table scans; slow queries | EXPLAIN shows no partition filter | Add partition column to WHERE clause |
| **Skewed data** | Some tasks run long; others fast | Task time variance; some tasks 10x slower | Enable AQE; consider salting join keys |
| **Cold cache** | First query of day slow | Repeating queries hit cache on 2nd run | Use `REFRESH TABLE` for critical tables |
| **Missing statistics** | Bad join order; slow query | EXPLAIN shows high cardinality estimates | `ANALYZE TABLE` to collect stats |

---

## Performance Tuning Checklist

- ✅ Z-order on common join keys (customer_id, date, product_id)
- ✅ Optimize partition strategy (not too many, not too few)
- ✅ Enable auto-compaction for Silver/Gold tables
- ✅ Collect table statistics (`ANALYZE TABLE`)
- ✅ Use broadcast joins for small dimensions (< 1GB)
- ✅ Enable Adaptive Query Execution (AQE)
- ✅ Pre-join common table combinations in Gold layer
- ✅ Partition by date/time for retention policies
- ✅ Monitor query execution plans (EXPLAIN)
- ✅ Schedule OPTIMIZE during off-peak hours
