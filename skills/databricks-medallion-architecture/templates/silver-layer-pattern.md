# Silver Layer Pattern - Data Transformation & Quality

## Silver Layer Design Principles

**Clean once, use everywhere** — Silver layer is the single source of truth. Implement business rules and quality checks once here, reuse in all downstream processes.

```
Bronze (Raw)
    ↓
Silver Transformations:
  ✓ Deduplication
  ✓ Data validation
  ✓ Schema standardization
  ✓ PII masking
  ✓ Type casting
    ↓
Gold (Analytics)
Analytics Teams (no direct Bronze access)
```

## Silver Layer Transformation Patterns

### Pattern 1: Deduplication & Incremental Processing

Use `MERGE INTO` for idempotent updates (key pattern in medallion):

```sql
-- Silver: Deduplicated customer master (SCD Type 1)
-- Merge strategy: Last write wins (simple case)

MERGE INTO silver.customers_deduped AS target
USING (
  SELECT
    customer_id,
    -- Take latest record by ingestion timestamp if duplicates exist
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingestion_timestamp DESC) as rn,
    customer_name,
    email_address,
    phone_number,
    date_created,
    date_modified,
    is_active
  FROM bronze.raw_customers
  WHERE _ingestion_date >= CURRENT_DATE() - 1  -- Only process recent data
) AS source
ON target.customer_id = source.customer_id AND source.rn = 1
WHEN MATCHED AND source.date_modified > target.date_modified THEN
  UPDATE SET
    customer_name = source.customer_name,
    email_address = source.email_address,
    phone_number = source.phone_number,
    is_active = source.is_active,
    updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    customer_name,
    email_address,
    phone_number,
    date_created,
    is_active,
    created_at,
    updated_at
  ) VALUES (
    source.customer_id,
    source.customer_name,
    source.email_address,
    source.phone_number,
    source.date_created,
    source.is_active,
    current_timestamp(),
    current_timestamp()
  );
```

### Pattern 2: Data Quality Validation with DLT

Use Databricks Delta Live Tables for declarative quality enforcement:

```python
# Silver: Data quality checks using DLT expectations

import dlt

@dlt.table(
  comment="Validated customer records with quality expectations",
  table_properties={"classification": "silver", "owner": "analytics_engineering"}
)
@dlt.expect_or_drop("valid_email", "email_address LIKE '%@%.%'")
@dlt.expect_or_drop("valid_phone", "phone_number IS NULL OR phone_number RLIKE '^\\+?[0-9]{10,}$'")
@dlt.expect_or_fail("customer_id_not_null", "customer_id IS NOT NULL")
def customers_validated():
  return spark.readStream.table("bronze.raw_customers") \
    .select(
      "customer_id",
      "customer_name",
      "email_address",
      "phone_number",
      "is_active",
      F.current_timestamp().alias("validated_at")
    )

# Monitor expectations
@dlt.table
def expectations_report():
  """Track data quality metrics"""
  return dlt.read("customers_validated") \
    .groupBy() \
    .agg(
      F.count("*").alias("total_records"),
      F.count(F.when(F.col("email_address").isNotNull(), 1)).alias("records_with_email"),
      F.count(F.when(F.col("is_active"), 1)).alias("active_customers")
    )
```

### Pattern 3: Schema Standardization & Type Casting

```sql
-- Silver: Customer unified (standardized schema from multiple sources)

CREATE TABLE silver.customers_unified (
  -- Standardized identifier (unique key)
  customer_id STRING NOT NULL COMMENT "Unique customer identifier",
  
  -- Name (standardized format)
  first_name STRING COMMENT "Customer first name",
  last_name STRING COMMENT "Customer last name",
  full_name GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)),
  
  -- Contact (normalized, validated)
  email STRING COMMENT "Normalized email (lowercase, trimmed)",
  phone_country_code STRING COMMENT "Country code (+1, +44, etc)",
  phone_number STRING COMMENT "Phone without country code",
  
  -- Addresses (normalized)
  street_address STRING,
  city STRING,
  state_province STRING,
  postal_code STRING,
  country_code STRING,
  
  -- Dates (standardized to UTC timestamp)
  date_of_birth DATE,
  account_created_date TIMESTAMP COMMENT "Account creation (UTC)",
  account_modified_date TIMESTAMP COMMENT "Last modification (UTC)",
  
  -- Flags
  is_active BOOLEAN DEFAULT true,
  opt_out_marketing BOOLEAN DEFAULT false,
  
  -- Audit
  source_system STRING COMMENT "Source system (oracle_crm, salesforce, etc)",
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'classification' = 'silver',
  'owner' = 'analytics_engineering',
  'retention_days' = '730',
  'pii_fields' = 'first_name,last_name,email,phone_number,date_of_birth'
);

-- Transformation query (Bronze → Silver)
INSERT INTO silver.customers_unified
SELECT
  customer_id,
  SPLIT(customer_name, ' ')[0] as first_name,      -- Parse name
  SPLIT(customer_name, ' ')[1] as last_name,
  LOWER(TRIM(email_address)) as email,             -- Normalize email
  REGEXP_EXTRACT(phone_number, '^\\+?([0-9]{1,3})') as phone_country_code,
  REGEXP_EXTRACT(phone_number, '[0-9]{10,}$') as phone_number,
  NULL as street_address,                          -- Not in source
  NULL as city,
  NULL as state_province,
  NULL as postal_code,
  NULL as country_code,
  NULL as date_of_birth,
  CAST(date_created AS TIMESTAMP) as account_created_date,
  CAST(date_modified AS TIMESTAMP) as account_modified_date,
  is_active,
  false as opt_out_marketing,
  _source_system as source_system,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
FROM bronze.raw_customers
WHERE _ingestion_date >= CURRENT_DATE() - 1;
```

### Pattern 4: PII Masking & Data Security

```python
# Silver: Apply PII masking for sensitive fields

from pyspark.sql import functions as F

def mask_pii(df, column_name, masking_char='*'):
  """Mask PII column except last 4 characters"""
  return df.withColumn(
    column_name,
    F.concat(
      F.lit(masking_char * 8),
      F.substring(F.col(column_name), -4, 4)
    )
  )

def hash_pii(df, column_name, salt='secret_salt'):
  """Hash PII using SHA-256"""
  from hashlib import sha256
  return df.withColumn(
    column_name,
    F.sha2(F.concat(F.col(column_name), F.lit(salt)), 256)
  )

# Example: Apply to sensitive columns
df_masked = mask_pii(df, 'phone_number')
df_hashed = hash_pii(df_masked, 'email')

df_hashed.write.mode('append').insertInto('silver.customers_masked')
```

### Pattern 5: Slowly Changing Dimension (SCD Type 2)

Track historical changes to customer attributes:

```sql
-- Silver: Customer profile with history (SCD Type 2)

CREATE TABLE silver.customer_profile_scd (
  customer_id STRING NOT NULL,
  email STRING NOT NULL,
  city STRING,
  country_code STRING,
  
  -- SCD Type 2 tracking
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  scd_version INT,
  
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('classification' = 'silver');

-- MERGE to implement SCD Type 2
MERGE INTO silver.customer_profile_scd AS target
USING (
  SELECT
    customer_id,
    email,
    city,
    country_code,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingestion_timestamp DESC) as rn
  FROM bronze.raw_customers
  WHERE _ingestion_date >= CURRENT_DATE() - 1
) AS source
ON target.customer_id = source.customer_id AND target.is_current = true AND source.rn = 1
WHEN MATCHED AND (
  source.email != target.email OR 
  source.city != target.city OR 
  source.country_code != target.country_code
) THEN
  -- Mark existing record as inactive
  UPDATE SET 
    is_current = false,
    valid_to = current_timestamp(),
    updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  -- Insert new record
  INSERT (
    customer_id, email, city, country_code,
    valid_from, valid_to, is_current, scd_version,
    created_at, updated_at
  ) VALUES (
    source.customer_id, source.email, source.city, source.country_code,
    current_timestamp(), NULL, true, 1,
    current_timestamp(), current_timestamp()
  );

-- Insert new version if change detected
INSERT INTO silver.customer_profile_scd
SELECT
  customer_id,
  email,
  city,
  country_code,
  current_timestamp() as valid_from,
  NULL as valid_to,
  true as is_current,
  (SELECT COALESCE(MAX(scd_version), 0) + 1 FROM silver.customer_profile_scd 
   WHERE customer_id = source.customer_id) as scd_version,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
FROM source
WHERE source.rn = 1
  AND (source.email, source.city, source.country_code) != 
      (SELECT email, city, country_code FROM silver.customer_profile_scd 
       WHERE customer_id = source.customer_id AND is_current = true);
```

## Silver Table Incremental Processing

```python
# Common pattern: Process only new/changed data from Bronze

def transform_incremental(bronze_table, silver_table, checkpoint_key):
  """
  Read new records from Bronze, transform, and upsert to Silver
  """
  # Get last processed timestamp
  try:
    last_processed = spark.sql(f"""
      SELECT MAX(updated_at) as last_update 
      FROM {silver_table}
    """).collect()[0]['last_update']
  except:
    last_processed = datetime(1970, 1, 1)  # First run

  # Read only new/changed records
  df_new = spark.sql(f"""
    SELECT * FROM {bronze_table}
    WHERE _ingestion_timestamp > '{last_processed}'
  """)

  # Apply transformations
  df_transformed = df_new.select(
    col("customer_id"),
    lower(trim(col("email_address"))).alias("email"),
    col("phone_number").alias("phone"),
    current_timestamp().alias("transformed_at")
  )

  # Write with MERGE (upsert)
  df_transformed.write.format("delta").mode("append").insertInto(silver_table)

  print(f"Processed {df_transformed.count()} new records")
```

## Silver Data Quality Monitoring

```sql
-- Monitor transformation quality metrics

CREATE TABLE monitoring.silver_quality_metrics (
  table_name STRING,
  metric_date DATE,
  total_records LONG,
  null_counts MAP<STRING, LONG>,
  duplicate_count LONG,
  validation_errors_count LONG,
  transformation_timestamp TIMESTAMP
);

-- Insert metrics
INSERT INTO monitoring.silver_quality_metrics
SELECT
  'silver.customers_unified' as table_name,
  CURRENT_DATE() as metric_date,
  COUNT(*) as total_records,
  MAP(
    'customer_id', COUNT(CASE WHEN customer_id IS NULL THEN 1 END),
    'email', COUNT(CASE WHEN email IS NULL THEN 1 END)
  ) as null_counts,
  COUNT(*) - COUNT(DISTINCT customer_id) as duplicate_count,
  COUNT(CASE WHEN email NOT LIKE '%@%.%' THEN 1 END) as validation_errors_count,
  CURRENT_TIMESTAMP() as transformation_timestamp
FROM silver.customers_unified
WHERE updated_at >= CURRENT_DATE();
```

## Silver Best Practices Checklist

- ✅ Use `MERGE INTO` for idempotent incremental processing
- ✅ Implement data quality checks (DLT expectations)
- ✅ Apply PII masking/hashing for sensitive columns
- ✅ Standardize schemas and data types
- ✅ Separate tables by source system (easier troubleshooting)
- ✅ Include audit columns (created_at, updated_at, source_system)
- ✅ Implement SCD Type 2 for dimension tables
- ✅ Document data contracts (valid values, ranges, relationships)
- ✅ Monitor transformation quality metrics
- ✅ Partition by business dates for efficient queries
- ✅ Use Z-order by common join keys (customer_id, order_id)
- ✅ Implement row-level security (RLS) for multi-tenant data
