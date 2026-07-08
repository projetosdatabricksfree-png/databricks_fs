# Medallion Architecture - Folder & Naming Convention

## Databricks Unity Catalog Structure

```yaml
# Workspace organization for medallion architecture

workspace: production-data-platform
  catalog: data_platform
    database: bronze
      tables:
        - raw_events             # /bronze/raw_events (external source: Kafka)
        - raw_customer_master    # /bronze/raw_customer_master (external source: Oracle CRM)
        - raw_transactions       # /bronze/raw_transactions (external source: Daily SFTP)
        - staging_errors         # Quarantine table for quality failures

    database: silver
      tables:
        - events_validated       # Deduplicated, schema-aligned events
        - customers_unified      # Merged customer data from multiple sources
        - transactions_cleansed  # Validated transaction records
        - customer_profile_scd   # Slowly Changing Dimension (Type 2)

    database: gold
      tables:
        - daily_revenue_by_product
        - customer_lifetime_value
        - monthly_churn_prediction_features
        - dim_date               # Conformed dimension
        - dim_customer           # Conformed dimension
        - fact_orders            # Fact table (grain: order line item)

    database: external_shares   # Delta Sharing for third parties
      tables:
        - public_metrics
        - shared_customer_segment

    database: monitoring        # Data quality & observability
      tables:
        - data_quality_metrics
        - pipeline_execution_log
        - lineage_tracking
```

## File Storage Organization (External Delta Tables)

```
ADLS Gen2 Container Structure:
/data-platform/
  /bronze/
    /raw_events/
      /year=2025/month=01/day=15/
        *.parquet
    /raw_customer_master/
      /version=current/
        *.parquet
  /silver/
    /events_validated/
      /year=2025/month=01/
        *.delta  (native Delta Lake format)
    /customers_unified/
      /year=2025/quarter=Q1/
        *.delta
  /gold/
    /daily_revenue_by_product/
      /year=2025/
        *.delta
```

## Naming Conventions

### Table Naming

| Layer | Pattern | Example | Notes |
|-------|---------|---------|-------|
| Bronze | `raw_<source_system>_<entity>` | `raw_oracle_customers`, `raw_kafka_events` | Prefix: source system identifier |
| Silver | `<entity>_<stage>` | `customers_unified`, `transactions_cleansed` | Suffix: transformation type |
| Gold | `<metric/entity>_<grain>` | `daily_revenue_by_product`, `dim_customer` | Prefix: grain/fact/dimension |

### Column Naming

```sql
-- Standard naming conventions (snake_case)
customer_id              -- PK/FK identifiers
event_timestamp          -- Event time (UTC)
ingestion_timestamp      -- When data arrived (UTC)
source_system            -- Origin identifier
valid_from, valid_to     -- SCD Type 2 dates
_change_type             -- CDC: insert/update/delete (prefix underscore for metadata)
is_current               -- SCD: active flag
```

### Metadata Columns (Every Bronze Table Should Have)

```sql
-- Required metadata columns in Bronze
CREATE TABLE bronze.raw_events (
  _event_id STRING,
  _ingestion_timestamp TIMESTAMP,
  _ingestion_date DATE,
  _source_system STRING,
  _partition_year INT,
  _partition_month INT,
  _partition_day INT,
  -- ... actual columns ...
  event_data STRING,
  event_timestamp TIMESTAMP
)
PARTITIONED BY (_partition_year, _partition_month, _partition_day)
CLUSTERED BY (_source_system) INTO 8 BUCKETS
TBLPROPERTIES (
  'delta.autoCompact' = 'true',
  'delta.targetFileSize' = '100mb',
  'delta.columnMapping.mode' = 'name',
  'classification' = 'bronze',
  'owner' = 'data_engineering'
);
```

## Partitioning Strategy

### Bronze Layer
- **Partition on**: Year, Month, Day (for retention policies)
- **Reasoning**: Enables fast deletion of old partitions; matches ingestion flow

```sql
PARTITION BY (year, month, day)
-- Example: /year=2025/month=01/day=15/
```

### Silver Layer
- **Partition on**: Year, Quarter (or Month if high volume)
- **Reasoning**: Balances query pruning with partition count

```sql
PARTITION BY (year, quarter)
-- Example: /year=2025/quarter=Q1/
```

### Gold Layer
- **Partition on**: Year (or no partition if pre-aggregated)
- **Reasoning**: Data already aggregated; minimal partitions for maintenance

```sql
PARTITION BY (year)
-- Example: /year=2025/
```

## File Size & Compaction Targets

| Layer | Target File Size | Auto-Compact | Z-Order | Reason |
|-------|---|---|---|---|
| Bronze | 100–200 MB | No (preserve raw chunks) | None | Maximize storage efficiency |
| Silver | 50–100 MB | Yes | On join keys | Balance query and write performance |
| Gold | 25–50 MB | Yes | On fact keys | Optimize for BI tool scans |

## Database Schema Separation Pattern

Use separate schemas (databases) by data domain:

```yaml
# Recommended approach: Schema-per-domain

catalog: data_platform
  schema: bronze               # All raw data
  schema: silver_marketing     # Marketing transformations
  schema: silver_finance       # Finance transformations
  schema: silver_operations    # Operations transformations
  schema: gold_analytics       # Shared analytics metrics
  schema: gold_ml              # ML features
```

**Benefits:**
- Clear ownership (Marketing team owns `silver_marketing`)
- Easier access control (grant role to entire schema)
- Reduced naming conflicts
- Simpler schema evolution (add columns to domain schema)

## Delta Table Optimization Settings

```python
# Recommended Databricks SQL settings for medallion tables

# Bronze (Raw - Preserve for replay)
ALTER TABLE bronze.raw_events
SET TBLPROPERTIES (
  'delta.autoCompact' = 'false',           # Manual compaction
  'delta.autoOptimize.optimizeWrite' = 'false',  # No Z-order overhead
  'delta.dataChangeFile' = 'true'          # Track changes for CDCV
);

# Silver (Quality & Transformations)
ALTER TABLE silver.customers_unified
SET TBLPROPERTIES (
  'delta.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.targetFileSize' = '100mb',
  'delta.appendOnly' = 'false'
);

# Gold (Analytics - Performance)
ALTER TABLE gold.daily_revenue_by_product
SET TBLPROPERTIES (
  'delta.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.targetFileSize' = '50mb',
  'delta.columnMapping.mode' = 'name'
);
```

## Data Classification Tags

Apply to all tables for governance:

```sql
ALTER TABLE bronze.raw_events
SET TBLPROPERTIES (
  'data_classification' = 'internal',
  'data_owner' = 'data_engineering_team',
  'retention_days' = '90',
  'pii_fields' = 'none'
);

ALTER TABLE silver.customers_unified
SET TBLPROPERTIES (
  'data_classification' = 'confidential',
  'data_owner' = 'analytics_engineering',
  'retention_days' = '730',  -- 2 years
  'pii_fields' = 'customer_id,email,phone'
);

ALTER TABLE gold.daily_revenue_by_product
SET TBLPROPERTIES (
  'data_classification' = 'public',
  'data_owner' = 'finance_team',
  'retention_days' = 'forever',
  'pii_fields' = 'none'
);
```
