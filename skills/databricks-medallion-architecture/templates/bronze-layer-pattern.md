# Bronze Layer Pattern - Raw Data Ingestion

## Bronze Layer Design Principles

**Speed over transformation** — Bronze is optimized for reliable, fast data capture. Transformation is deferred to Silver layer.

```
Raw Data Source → Auto Loader / Streaming → Bronze Table
                                              ↓
                                    Metadata columns added
                                    ↓ (no other transformation)
                                    Store partition for retention
```

## Bronze Ingestion Pattern: Auto Loader + Delta

Use Databricks Auto Loader for reliable file ingestion with schema inference:

```python
# Bronze: Raw file ingestion using Auto Loader
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# Option 1: Schema inference (simpler, slower first run)
df = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
  .option("cloudFiles.schemaLocation", "/tmp/schema_location") \
  .option("cloudFiles.inferColumnTypes", "true") \
  .load("adls://container/source/daily_extract/")

# Option 2: Explicit schema (faster, requires schema management)
schema = StructType([
  StructField("customer_id", StringType()),
  StructField("order_date", TimestampType()),
  StructField("amount", LongType()),
  # ... add all source columns ...
])

df = spark.readStream \
  .format("cloudFiles") \
  .schema(schema) \
  .option("cloudFiles.format", "csv") \
  .load("adls://container/source/")

# Add metadata columns
from pyspark.sql.functions import col, current_timestamp, lit
import datetime

df_with_metadata = df.select(
  col("*"),  # All original columns
  current_timestamp().alias("_ingestion_timestamp"),
  lit(datetime.date.today()).alias("_ingestion_date"),
  lit("oracle_crm").alias("_source_system"),
  lit(datetime.date.today().year).alias("_partition_year"),
  lit(datetime.date.today().month).alias("_partition_month"),
  lit(datetime.date.today().day).alias("_partition_day")
)

# Write to Bronze with checkpointing for exactly-once semantics
df_with_metadata.writeStream \
  .format("delta") \
  .option("checkpointLocation", "/tmp/checkpoint/crm_customers") \
  .option("path", "abfss://container@storage.dfs.core.windows.net/bronze/raw_customers") \
  .mode("append") \
  .partitionBy("_partition_year", "_partition_month", "_partition_day") \
  .start()
```

## Bronze Ingestion Pattern: Kafka Streaming

For real-time event streams:

```python
# Bronze: Kafka streaming ingestion
kafka_options = {
  "kafka.bootstrap.servers": "kafka-broker:9092",
  "subscribe": "user_events_topic",
  "startingOffsets": "earliest",
  "failOnDataLoss": "false"  # Resume from checkpoint
}

df = spark.readStream \
  .format("kafka") \
  .options(**kafka_options) \
  .load()

# Parse Kafka value (JSON) into columns
from pyspark.sql.functions import from_json, col

schema = "event_id STRING, user_id STRING, event_type STRING, properties STRING, timestamp LONG"

df_parsed = df.select(
  from_json(col("value").cast("string"), schema).alias("event")
).select(
  "event.*",
  col("timestamp").alias("_kafka_timestamp"),
  current_timestamp().alias("_ingestion_timestamp"),
  lit("kafka").alias("_source_system"),
  year(current_timestamp()).alias("_partition_year"),
  month(current_timestamp()).alias("_partition_month"),
  dayofmonth(current_timestamp()).alias("_partition_day")
)

df_parsed.writeStream \
  .format("delta") \
  .option("checkpointLocation", "/tmp/checkpoint/events") \
  .option("path", "abfss://container@storage.dfs.core.windows.net/bronze/raw_events") \
  .mode("append") \
  .partitionBy("_partition_year", "_partition_month", "_partition_day") \
  .start()
```

## Bronze Table Schema Definition

```sql
-- Bronze: Raw customer data from Oracle CRM
CREATE TABLE bronze.raw_customers (
  -- Metadata columns (required in all Bronze tables)
  _ingestion_timestamp TIMESTAMP COMMENT "When record was ingested",
  _ingestion_date DATE COMMENT "Ingestion date for retention",
  _source_system STRING COMMENT "Source system identifier (oracle_crm, salesforce, etc)",
  _partition_year INT,
  _partition_month INT,
  _partition_day INT,
  
  -- Source columns (as-is, no transformation)
  customer_id STRING COMMENT "Primary identifier from source",
  customer_name STRING COMMENT "Customer full name",
  email_address STRING COMMENT "Email (raw, may contain duplicates)",
  phone_number STRING COMMENT "Phone (raw format)",
  date_created TIMESTAMP COMMENT "Date created in source system",
  date_modified TIMESTAMP COMMENT "Last modification in source",
  is_active BOOLEAN COMMENT "Active flag in source",
  raw_metadata STRING COMMENT "Any additional JSON metadata from source"
)
USING DELTA
PARTITIONED BY (_partition_year, _partition_month, _partition_day)
TBLPROPERTIES (
  'delta.autoCompact' = 'false',
  'delta.autoOptimize.optimizeWrite' = 'false',
  'classification' = 'bronze',
  'retention_days' = '90',
  'owner' = 'data_engineering',
  'source_system' = 'oracle_crm',
  'description' = 'Raw customer master data ingested from Oracle CRM'
);
```

## Bronze Ingestion Workflow (Batch Daily Load)

```python
# Typical workflow: Load daily batch of data

from pyspark.sql import functions as F
from datetime import datetime, timedelta

# 1. Read source file
source_date = (datetime.now() - timedelta(days=1)).date()  # Yesterday's data
source_path = f"abfss://container@storage.dfs.core.windows.net/source/customers/{source_date}.parquet"

df = spark.read.parquet(source_path)

# 2. Add metadata + checksums for deduplication
df_with_metadata = df.select(
  "*",
  F.current_timestamp().alias("_ingestion_timestamp"),
  F.lit(source_date).alias("_ingestion_date"),
  F.lit("oracle_crm").alias("_source_system"),
  F.year(F.lit(source_date)).alias("_partition_year"),
  F.month(F.lit(source_date)).alias("_partition_month"),
  F.dayofmonth(F.lit(source_date)).alias("_partition_day"),
  F.md5(F.concat_ws("|", "*")).alias("_content_hash")  # For dedup detection
)

# 3. Write to Bronze (append mode = immutable history)
df_with_metadata.write \
  .format("delta") \
  .mode("append") \
  .insertInto("bronze.raw_customers")

# 4. Log ingestion metadata
from pyspark.sql import Row
ingestion_log = spark.createDataFrame([
  Row(
    table_name="bronze.raw_customers",
    ingestion_date=source_date,
    row_count=df_with_metadata.count(),
    ingestion_timestamp=datetime.now(),
    status="success"
  )
])

ingestion_log.write \
  .format("delta") \
  .mode("append") \
  .insertInto("monitoring.ingestion_log")
```

## Data Quality Quarantine Pattern

```sql
-- Quarantine table for records that fail validation
CREATE TABLE bronze.raw_customers_quarantine (
  _quarantine_timestamp TIMESTAMP,
  _quarantine_reason STRING,
  _quarantine_error_detail STRING,
  _original_record STRING,  -- Full JSON of original record
  _source_system STRING,
  _partition_year INT,
  _partition_month INT,
  _partition_day INT
)
USING DELTA
PARTITIONED BY (_partition_year, _partition_month, _partition_day);

-- Move records to quarantine if they fail schema validation
INSERT INTO bronze.raw_customers_quarantine
SELECT
  current_timestamp() as _quarantine_timestamp,
  'schema_mismatch' as _quarantine_reason,
  col('error_message').cast('STRING') as _quarantine_error_detail,
  to_json(struct('*')) as _original_record,
  'oracle_crm' as _source_system,
  year(current_date()) as _partition_year,
  month(current_date()) as _partition_month,
  dayofmonth(current_date()) as _partition_day
FROM invalid_records_view;
```

## Bronze Data Retention Policy

```python
# Automate Bronze partition cleanup (retention = 90 days)
from datetime import datetime, timedelta

def purge_old_bronze_partitions(table_name, retention_days=90):
  """Delete partitions older than retention_days"""
  cutoff_date = (datetime.now() - timedelta(days=retention_days)).date()
  
  partition_column = "_ingestion_date"
  
  spark.sql(f"""
    DELETE FROM {table_name}
    WHERE {partition_column} < '{cutoff_date}'
  """)
  
  spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({partition_column})")
  
  print(f"Purged {table_name} partitions older than {cutoff_date}")

# Run weekly
purge_old_bronze_partitions("bronze.raw_customers", retention_days=90)
purge_old_bronze_partitions("bronze.raw_transactions", retention_days=90)
```

## Monitoring Bronze Ingestion

```sql
-- Monitor ingestion SLAs
SELECT
  table_name,
  DATE(ingestion_timestamp) as ingestion_date,
  COUNT(*) as record_count,
  COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_ingestions,
  COUNT(CASE WHEN status = 'failure' THEN 1 END) as failed_ingestions,
  MAX(ingestion_timestamp) as last_ingestion_time
FROM monitoring.ingestion_log
WHERE table_name LIKE 'bronze.%'
GROUP BY table_name, DATE(ingestion_timestamp)
ORDER BY ingestion_date DESC;

-- Check for duplicate detection
SELECT
  _source_system,
  _ingestion_date,
  COUNT(*) as total_records,
  COUNT(DISTINCT customer_id) as unique_customers,
  (COUNT(*) - COUNT(DISTINCT customer_id)) as duplicate_records
FROM bronze.raw_customers
WHERE _partition_year = YEAR(CURRENT_DATE())
  AND _partition_month = MONTH(CURRENT_DATE())
GROUP BY _source_system, _ingestion_date;
```

## Bronze Best Practices Checklist

- ✅ Add metadata columns to every Bronze table (ingestion_timestamp, source_system, etc.)
- ✅ Partition by date for retention policies
- ✅ Use checksums/content hashes to detect duplicates early
- ✅ Implement quarantine tables for failed records
- ✅ Enable exactly-once semantics with checkpointing (for streaming)
- ✅ Never delete Bronze data (immutable audit trail) — use retention policies instead
- ✅ Monitor ingestion metrics (row counts, latency, error rates)
- ✅ Document source system contracts (schema, update frequency, SLAs)
- ✅ Separate Bronze tables by source system (easier troubleshooting)
- ✅ Use Auto Loader for reliable file ingestion with automatic schema evolution
