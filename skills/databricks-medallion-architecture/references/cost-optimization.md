# Cost Optimization for Medallion Architecture

## Layer-by-Layer Cost Drivers

| Layer | Storage Cost | Compute Cost | Optimization |
|-------|---|---|---|
| **Bronze** | 60-70% of cost (raw data retention) | 20% (simple ingestion) | Retention policies; compression; tiering |
| **Silver** | 20% (transformed, deduplicated) | 40% (complex transforms) | Incremental processing; Z-order; partition pruning |
| **Gold** | 10% (pre-aggregated, small) | 40% (BI/analytics scans) | Pre-aggregation; denormalization; query optimization |

---

## Storage Cost Optimization

### Retention Policies (Bronze)

```sql
-- Delete old Bronze partitions automatically (saves 60-70% of storage)

-- Manual: Delete Bronze partitions older than 90 days
DELETE FROM bronze.raw_events
WHERE _ingestion_date < CURRENT_DATE() - 90;

-- Automated: Create retention job
CREATE JOB bronze_retention_cleanup AS
  SELECT * FROM bronze.raw_events
  WHERE _ingestion_date >= CURRENT_DATE() - 90
INTO TABLE bronze.raw_events_retained
USING DELTA;

-- Typical savings: 90-day retention → $50K/month on 10TB Bronze layer
```

### Data Compression

```python
# Enable Parquet/Delta compression (default: snappy)

df.write \
  .format("delta") \
  .option("compression", "snappy") \  # or "gzip" for 50% smaller (slower)
  .mode("append") \
  .insertInto("bronze.raw_events")

# Compression ratio:
# - snappy: 60-70% (fast; good CPU trade-off)
# - gzip: 40-50% (slow; best compression)
# - zstd: 50-60% (balance)
```

### Tiered Storage (Archive)

```python
# Move old Bronze data to Azure Cool/Archive tier

import boto3

# Archive partitions older than 6 months
for year, month in [(2023, 1), (2023, 2)]:
  partition_path = f"s3://datalake/bronze/raw_events/year={year}/month={month}/"
  
  # Move to Cool tier (lower cost; slower access)
  archive_object(partition_path, tier="cool")
  
  # Cost savings: Cool tier ~50% cheaper than Hot
```

### Deduplication (Silver)

```sql
-- Remove duplicates → reduce storage 10-20%

CREATE TABLE silver.customers_deduped AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingestion_timestamp DESC) as rn
  FROM bronze.raw_customers
)
WHERE rn = 1;

-- Storage before: 100GB
-- Storage after: 80GB (20% reduction)
```

---

## Compute Cost Optimization

### Incremental Processing (Don't Re-process Everything)

```sql
-- ❌ BAD: Full table re-processing (compute cost = $1000/day)
INSERT OVERWRITE silver.customers_unified
SELECT * FROM bronze.raw_customers;

-- ✅ GOOD: Incremental - only new/changed (compute cost = $100/day)
MERGE INTO silver.customers_unified AS target
USING (
  SELECT * FROM bronze.raw_customers
  WHERE _ingestion_date >= CURRENT_DATE() - 1  -- Only yesterday's data
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

-- Annual savings: ($1000 - $100) * 365 = $328,500
```

### Partition Pruning (Scan Less Data)

```sql
-- ❌ BAD: Full table scan ($50/query)
SELECT * FROM silver.transactions WHERE amount > 1000;

-- ✅ GOOD: Partition-pruned query ($5/query)
SELECT * FROM silver.transactions
WHERE year = 2024 AND month = 4  -- Partition columns
  AND amount > 1000;

-- Cost per 1000 queries: $50,000 vs. $5,000 = $45,000 saved
```

### Serverless SQL Endpoints (Pay-per-Query)

```sql
-- Use Serverless SQL Endpoints instead of reserved clusters
-- Pricing: $0.40/DBU (compute unit)

-- Typical workload: 100 queries/day, 10 minutes each
-- Reserved cluster: 4 workers @ $0.40/DBU = $250/day = $91,250/year
-- Serverless endpoint: Scale automatically, pay only for compute = $30,000/year

-- Recommendation: Switch if query patterns are bursty (not 24/7)
```

### Query Optimization (Reduce Compute Time)

```sql
-- ❌ SLOW QUERY (15 minutes; cost = $10)
SELECT
  region,
  product_id,
  COUNT(*) as orders,
  SUM(amount) as revenue
FROM fact_orders f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
WHERE date >= CURRENT_DATE() - 30
GROUP BY region, product_id;

-- ✅ OPTIMIZED (1 minute; cost = $0.67)
-- (Pre-join in gold; partition pruning; denormalization)
SELECT
  region,
  product_id,
  COUNT(*) as orders,
  SUM(amount) as revenue
FROM gold.revenue_summary  -- Pre-aggregated daily
WHERE date >= CURRENT_DATE() - 30
GROUP BY region, product_id;

-- Annual savings at 1000 queries/year: (10 - 0.67) * 1000 = $9,330
```

---

## Retention Policies by Layer

### Bronze: Aggressive Retention (Minimize Storage)

```yaml
retention_strategy: 90_days_rolling_window

rationale:
  - Purpose: Replay & debugging (not long-term archive)
  - Cost driver: Storage (60-70% of medallion cost)
  - Recovery: Can always re-ingest from source

implementation:
  - Retention: Delete partitions older than 90 days
  - Archive: No archival (data in source systems)
  - Cost impact: 90-day policy saves ~$50K/month on 10TB
```

### Silver: Moderate Retention (Compliance + Analytics)

```yaml
retention_strategy: 2_years_with_quarterly_summarization

rationale:
  - Purpose: Compliance, historical analysis, SCD tracking
  - Cost driver: Storage (20% of medallion cost)
  - Compliance: May be required (GDPR, SOX)

implementation:
  - Retention: Keep 2 years of detail
  - Summarization: Quarterly aggregates > 1 year (compress storage)
  - Archive: None (data is transformed, not raw)
  - Cost impact: Pre-aggregate quarterly → 40% storage savings
```

### Gold: Permanent Retention (Historical Data)

```yaml
retention_strategy: permanent_with_yearly_archival

rationale:
  - Purpose: Historical analytics, trend analysis
  - Cost driver: Query cost (BI tools scan these tables)
  - Compliance: Permanent record for auditing

implementation:
  - Retention: Keep forever
  - Archival: Move yearly data to Cool tier if rarely queried
  - Partitioning: By year (easy to age out)
  - Cost impact: Archive 2+ year-old data → $5K/year savings
```

---

## Cost Monitoring & Allocation

### Track Cost by Layer

```sql
-- Monitor storage by layer
SELECT
  table_schema as layer,
  SUM(size_in_bytes) / 1099511627776.0 as size_gb,
  SUM(size_in_bytes) / 1099511627776.0 * 0.023 as monthly_storage_cost  -- $0.023/GB/month
FROM system.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
GROUP BY table_schema
ORDER BY size_gb DESC;

-- Sample output:
-- Layer   | Size (GB) | Monthly Cost
-- Bronze  | 800       | $18,400
-- Silver  | 200       | $4,600
-- Gold    | 100       | $2,300
-- Total   |1100       | $25,300/month
```

### Track Compute by Job

```python
# Use Databricks Compute Cost Analysis

from databricks.sdk import WorkspaceClient
import json

ws = WorkspaceClient()

# Get cluster costs
clusters = ws.clusters.list()
for cluster in clusters:
  print(f"Cluster: {cluster.cluster_name}")
  print(f"DBU rate: ${cluster.aws_attributes.dbu_per_hour}")
  print(f"Annual cost: ${cluster.aws_attributes.dbu_per_hour * 24 * 365}")

# Cost estimate: 4-worker cluster @ $0.40/DBU
# 24/7 cluster = $0.40 * 24 * 365 * 4 workers = ~$14,000/year
```

### Chargeback Model (Cost Allocation to Teams)

```json
{
  "cost_allocation": {
    "data_engineering_team": {
      "bronze_ingestion": 18400,
      "silver_transformations": 4600,
      "total_monthly": 23000
    },
    "analytics_team": {
      "gold_queries": 2300,
      "bi_dashboards": 500,
      "total_monthly": 2800
    },
    "data_science_team": {
      "ml_feature_computation": 1500,
      "total_monthly": 1500
    },
    "total_monthly_cost": 27300,
    "allocation_method": "by_table_ownership"
  }
}
```

---

## Cost Optimization Checklist

### Storage Optimization
- ✅ Set Bronze retention = 90 days (delete old partitions weekly)
- ✅ Set Silver retention = 2 years (summarize yearly)
- ✅ Archive Gold 2+ year-old data to Cool tier
- ✅ Enable compression (snappy for balance)
- ✅ Remove duplicate records (10-20% savings)
- ✅ Partition tables by date for retention policies
- ✅ Monitor storage by layer monthly

### Compute Optimization
- ✅ Use incremental processing (MERGE, not INSERT OVERWRITE)
- ✅ Add partition filters to WHERE clauses
- ✅ Pre-aggregate complex queries in Gold layer
- ✅ Use broadcast joins for small dimensions
- ✅ Enable partition pruning in query plans
- ✅ Schedule jobs during off-peak (lower pricing)
- ✅ Use Serverless SQL for bursty workloads
- ✅ Right-size cluster (not over-provisioned)

### Monitoring
- ✅ Track cost by layer (Bronze, Silver, Gold)
- ✅ Track cost by team (chargeback)
- ✅ Set budgets per layer / per team
- ✅ Alert on cost spikes (> 10% month-over-month)
- ✅ Review & optimize top 10 expensive queries monthly

---

## Cost Reduction Targets by Scenario

### Scenario 1: 10TB Raw Data (High Retention)

| Optimization | Impact | Annual Savings |
|---|---|---|
| Reduce Bronze retention to 60 days | -20% storage | $40K |
| Enable compression | -30% storage | $60K |
| Incremental Silver processing | -70% compute | $80K |
| Partition pruning in queries | -50% compute | $100K |
| **Total** | | **$280K/year** |

### Scenario 2: BI/Analytics Heavy (High Query Cost)

| Optimization | Impact | Annual Savings |
|---|---|---|
| Pre-aggregate metrics in Gold | -60% query cost | $120K |
| Broadcast small dimensions | -40% shuffle cost | $50K |
| Move to Serverless endpoints | -40% cluster cost | $100K |
| Query result caching | -70% repeat queries | $80K |
| **Total** | | **$350K/year** |

### Scenario 3: Real-time Streaming Ingestion

| Optimization | Impact | Annual Savings |
|---|---|---|
| Auto-scale clusters (not always-on) | -30% compute | $80K |
| Batch streaming micro-batches | -40% overhead | $50K |
| Incremental Silver processing | -60% compute | $100K |
| Archive old Bronze data | -40% storage | $60K |
| **Total** | | **$290K/year** |

---

## References

- [Databricks Pricing Calculator](https://databricks.com/pricing)
- [Delta Lake Performance Tuning](https://docs.databricks.com/en/delta/optimizations/index.html)
- [Unity Catalog Cost Analysis](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
