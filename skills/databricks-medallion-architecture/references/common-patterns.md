# Common Medallion Architecture Patterns

Real-world examples of medallion architecture by industry and use case.

---

## Pattern 1: E-Commerce Platform (Orders + Customers)

**Requirement:** Real-time order processing + customer analytics

### Architecture

```
Raw Data Sources:
├─ Kafka: Order events (real-time)
├─ PostgreSQL: Customer master (batch daily)
└─ S3: Product catalog (batch hourly)

Bronze Layer:
├─ raw_orders (from Kafka)
├─ raw_customers (from PostgreSQL)
└─ raw_products (from S3)

Silver Layer:
├─ orders_validated (dedup, quality checks)
├─ customers_unified (join CRM + ops systems)
└─ products_standardized

Gold Layer:
├─ fact_orders (order line items)
├─ dim_customer
├─ dim_product
├─ dim_date
└─ daily_revenue_summary (pre-aggregated)
```

### Key Design Decisions

| Decision | Choice | Reasoning |
|----------|--------|-----------|
| Order ingestion | Streaming (Kafka) + batch (hourly) | Real-time ordering; hourly reconciliation |
| Customer freshness | 24-hour batch | CRM updates nightly |
| Bronze retention | 30 days | Replay window for orders |
| Silver retention | 2 years | Historical order analysis |
| Gold pre-aggregation | Daily revenue by product/region | Pre-computed BI metrics |
| Partition strategy | date, region, product_category | Query filters often use these |

### PySpark Implementation (Silver → Gold)

```python
from pyspark.sql import functions as F

# Read Silver tables
orders = spark.read.table("silver.orders_validated")
customers = spark.read.table("silver.customers_unified")

# Create Fact table (Gold)
fact_orders = orders.select(
  F.col("order_id"),
  F.col("customer_id"),
  F.col("product_id"),
  F.col("order_date").alias("date"),
  F.col("amount"),
  F.col("quantity")
)

fact_orders.write.mode("overwrite").insertInto("gold.fact_orders")

# Create daily revenue summary (pre-aggregated)
daily_revenue = orders \
  .groupBy(
    F.col("order_date").alias("date"),
    F.col("region"),
    F.col("product_category")
  ) \
  .agg(
    F.count("*").alias("order_count"),
    F.sum("amount").alias("total_revenue"),
    F.avg("amount").alias("avg_order_value")
  )

daily_revenue.write.mode("append").insertInto("gold.daily_revenue_summary")
```

---

## Pattern 2: Financial Services (Transactions + Compliance)

**Requirement:** Transaction processing with regulatory audit trail, fraud detection

### Architecture

```
Raw Data Sources:
├─ Oracle Financials: Transactions (batch hourly)
├─ Kafka: Fraud signals (real-time)
└─ SWIFT: Wire transfers (batch daily)

Bronze Layer:
├─ raw_transactions (with metadata)
├─ raw_fraud_signals
└─ raw_wire_transfers
   ↓
Silver Layer:
├─ transactions_validated (SCD Type 2 for corrections)
├─ fraud_indicators_enriched
└─ wire_transfer_reconciled
   ↓
Gold Layer:
├─ fact_transactions
├─ dim_customer
├─ dim_compliance_rules
└─ fraud_probability_scores (ML features)
```

### Key Features

1. **Immutability (Compliance):** Bronze is never deleted; SCD Type 2 in Silver tracks corrections
2. **Audit Trail:** Every transaction has `audit_timestamp`, `audit_user`, `correction_type`
3. **Lineage:** Track which raw → silver → gold to prove data provenance
4. **Fraud Detection:** Join transaction patterns with fraud signals

### SCD Type 2 Implementation

```sql
-- Track transaction corrections for audit
MERGE INTO silver.transactions_with_audit AS target
USING (
  SELECT
    transaction_id,
    amount,
    correction_reason,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY update_timestamp DESC) as rn
  FROM bronze.raw_transactions
) AS source
ON target.transaction_id = source.transaction_id AND source.rn = 1
WHEN MATCHED AND source.correction_reason IS NOT NULL THEN
  -- Mark old version as inactive
  UPDATE SET
    valid_to = current_timestamp(),
    is_current = false,
    correction_applied = true
WHEN NOT MATCHED THEN
  -- Insert new corrected version
  INSERT (transaction_id, amount, valid_from, valid_to, is_current, scd_version)
  VALUES (source.transaction_id, source.amount, current_timestamp(), NULL, true, 1);
```

---

## Pattern 3: IoT / Telemetry (High-Volume Streaming)

**Requirement:** Billions of sensor events, anomaly detection, real-time alerting

### Architecture

```
Data Source: IoT sensors → MQTT → Kafka (billions/day)

Bronze Layer:
├─ raw_sensor_events (partitioned by device_id, timestamp)
   Retention: 7 days (cost reasons)

Silver Layer:
├─ events_deduplicated (remove duplicates)
├─ events_time_normalized (fix clock skew)
├─ sensor_readings_aggregated (5-min buckets)
   Retention: 90 days

Gold Layer:
├─ hourly_metrics_by_device
├─ daily_anomalies (ML predictions)
└─ realtime_alerts (filtered for downstream systems)
```

### Cost Optimization Strategies

```python
# 1. Aggressive compression (snappy → gzip)
df.write \
  .format("delta") \
  .option("compression", "gzip") \
  .mode("append") \
  .insertInto("bronze.raw_sensor_events")

# 2. Partition aggressively (device_id, date, hour)
# Bronze: 1 billion events/day → ~1000 partitions
# Enables parallel ingestion; easy cleanup

# 3. Aggregate to Silver hourly (not keeping raw)
aggregated = df \
  .groupBy(F.window(F.col("timestamp"), "1 hour"), F.col("device_id")) \
  .agg(F.avg("temperature").alias("avg_temp"))

# 4. Delete Bronze after 7 days
DELETE FROM bronze.raw_sensor_events
WHERE date < CURRENT_DATE() - 7
```

---

## Pattern 4: Healthcare (HIPAA Compliance + SCD)

**Requirement:** Patient records with history; HIPAA audit requirements

### Architecture

```
Raw Data Sources:
├─ EHR System: Patient records (batch daily)
├─ Lab System: Results (batch daily)
└─ Pharmacy: Medications (real-time)

Bronze Layer:
├─ raw_patient_records (encryption at rest)
├─ raw_lab_results
└─ raw_medications
   ↓
Silver Layer:
├─ patient_demographics_scd (Track address, insurance changes)
├─ lab_results_validated
└─ medications_active
   ↓
Gold Layer:
├─ fact_patient_visits
├─ dim_patient
├─ dim_provider
└─ patient_risk_scores (redacted)
```

### PII Handling

```python
# Hash patient ID for privacy
from pyspark.sql.functions import sha2, concat, lit

df_pii_hashed = df.withColumn(
  "patient_id_hash",
  sha2(concat(col("patient_id"), lit("hipaa_salt")), 256)
)

# Mask SSN
df_masked = df_pii_hashed.withColumn(
  "ssn_masked",
  concat(lit("***-**-"), substring(col("ssn"), -4, 4))
)

df_masked.write.insertInto("silver.patient_records_masked")
```

### Audit Logging

```sql
-- Track all access to patient data (HIPAA requirement)
CREATE TABLE monitoring.hipaa_audit_log (
  access_timestamp TIMESTAMP,
  user_id STRING,
  table_name STRING,
  patient_id_hash STRING,
  operation STRING,  -- SELECT, INSERT, UPDATE, DELETE
  row_count LONG
);

-- Enable audit logging on tables
ALTER TABLE silver.patient_demographics_scd
SET TBLPROPERTIES ('audit_logging_enabled' = 'true');
```

---

## Pattern 5: Marketing Analytics (Multi-tenant SaaS)

**Requirement:** Campaign analytics for multiple customers; tenant isolation

### Architecture

```
Raw Data Sources:
├─ Kafka: User events (from all customers)
├─ Segment API: Traits (all customers)
└─ S3: Campaign definitions (all customers)

Bronze Layer:
├─ raw_events (partitioned by tenant_id, date)

Silver Layer:
├─ events_per_tenant (schema: tenant_id, event_type, user_id, properties)

Gold Layer:
├─ fact_campaign_performance (per tenant)
├─ dim_user (per tenant)
└─ campaign_metrics_daily (pre-aggregated per tenant)
```

### Row-Level Security (RLS)

```sql
-- Enforce tenant isolation at query time
ALTER TABLE gold.campaign_metrics_daily
SET ROW SECURITY POLICY tenant_policy
GRANT (SELECT) ON gold.campaign_metrics_daily
TO marketing_user
USING (tenant_id = current_user_id());

-- Query automatically filtered
SELECT * FROM gold.campaign_metrics_daily
-- User 'customer_123' only sees tenant_id = 'customer_123'
```

### Multi-Tenant Partitioning

```python
# Partition by tenant for efficient filtering
df.write \
  .format("delta") \
  .mode("append") \
  .partitionBy("tenant_id", "date") \
  .insertInto("gold.campaign_metrics_daily")

# Query with partition pruning
SELECT * FROM gold.campaign_metrics_daily
WHERE tenant_id = 'acme_corp'  -- Single partition scanned
  AND date >= CURRENT_DATE() - 30
```

---

## Pattern 6: Streaming + Batch Hybrid (Lambda Architecture)

**Requirement:** Real-time + batch accuracy for high-volume data

### Architecture

```
Data Source: Events → Kafka

Speed Layer (Streaming/Real-time):
├─ Streaming ingestion → Bronze streaming table
├─ Silver transforms (aggregation)
└─ Gold real-time metrics (15-min refresh)

Batch Layer (Historical/Accuracy):
├─ Daily batch from S3 → Bronze batch table
├─ Silver: Dedupe, validate
└─ Gold: Merge batch + streaming results

Serving Layer:
├─ Real-time API: Query Gold streaming table
└─ Analytics: Query Gold batch-merged table
```

### Implementation (Delta Live Tables + Streaming)

```python
import dlt

# Speed layer: Streaming Bronze
@dlt.table
def bronze_streaming_events():
  return (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
    .load()
    .select(from_json(col("value"), event_schema).alias("data"))
  )

# Batch layer: Batch Bronze
@dlt.table
def bronze_batch_events():
  return spark.read.parquet("s3://bucket/batch_events/")

# Silver: Unified (streaming + batch)
@dlt.table
def silver_events_unified():
  streaming_df = dlt.read("bronze_streaming_events")
  batch_df = dlt.read("bronze_batch_events")
  
  return streaming_df.unionByName(batch_df)

# Gold: Real-time aggregation
@dlt.table
def gold_events_summary_realtime():
  return (
    dlt.read("silver_events_unified")
    .groupBy(F.window("timestamp", "15 minutes"))
    .agg(F.count("*").alias("event_count"))
  )
```

---

## Comparison Matrix: Which Pattern for Your Use Case?

| Use Case | Best Pattern | Key Considerations |
|----------|---|---|
| **E-commerce** | Pattern 1 | Real-time order processing; customer enrichment |
| **Financial** | Pattern 2 | Immutability; audit trail; SCD Type 2 |
| **IoT/Telemetry** | Pattern 3 | High volume; aggressive retention; compression |
| **Healthcare** | Pattern 4 | HIPAA compliance; PII masking; audit logging |
| **Multi-tenant SaaS** | Pattern 5 | Row-level security; partition isolation |
| **Hybrid Real-time** | Pattern 6 | Speed + batch accuracy; streaming + batch |

---

## References & Further Reading

- [Databricks Solutions: Industry Use Cases](https://databricks.com/solutions)
- [Delta Lake Best Practices](https://docs.databricks.com/en/delta/best-practices.html)
- [Unity Catalog Security Patterns](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [DLT (Delta Live Tables) Documentation](https://docs.databricks.com/en/workflows/delta-live-tables/index.html)
