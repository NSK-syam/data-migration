# Data Migration: Snowflake ↔ AWS S3 - Interview Guide

## 1. What is AWS S3?

**Amazon Simple Storage Service (S3)** is cloud object storage.

### Key Concepts:
- **Bucket**: A container for files (like a root folder)
- **Object**: Any file stored in S3 (CSV, Parquet, JSON, images, etc.)
- **Key**: The file path within a bucket (e.g., `data/orders/2024/orders.csv`)
- **Region**: Geographic location where data is stored (e.g., `us-east-1`)

### Why Use S3?
- Unlimited storage capacity
- 99.999999999% durability (11 nines)
- Cost-effective for large datasets
- Integrates with everything (Snowflake, Spark, Lambda, etc.)

### Interview Tip:
> "S3 is often used as a **data lake** - a central repository for raw data before processing."

---

## 2. What is Snowflake?

**Snowflake** is a cloud-based **data warehouse** for analytics.

### Key Concepts:
- **Warehouse**: Compute resources that run queries (can scale up/down)
- **Database**: Logical container for schemas
- **Schema**: Logical container for tables
- **Table**: Structured data with rows and columns

### Architecture (3 Layers):
```
┌─────────────────────────────────────┐
│  Cloud Services (Brain)             │  ← Query optimization, security
├─────────────────────────────────────┤
│  Virtual Warehouses (Compute)       │  ← Run queries, scale independently
├─────────────────────────────────────┤
│  Storage (Data)                     │  ← Compressed, columnar format
└─────────────────────────────────────┘
```

### Why Use Snowflake?
- Separates storage and compute (pay for what you use)
- Auto-scaling
- Near-zero maintenance
- Supports semi-structured data (JSON, Parquet)
- Time travel (query historical data)

### Interview Tip:
> "Snowflake's separation of storage and compute means we can scale query performance without duplicating data."

---

## 3. Why Migrate Data Between Them?

### S3 → Snowflake (Ingestion)
| Reason | Example |
|--------|---------|
| Analytics | Run SQL queries on raw data |
| Data Warehousing | Join with other business data |
| BI/Reporting | Connect Tableau, Power BI |
| Data Transformation | Clean and model data |

### Snowflake → S3 (Export)
| Reason | Example |
|--------|---------|
| Data Sharing | Send data to partners |
| Archival | Long-term cold storage (cheaper) |
| ML/AI Pipelines | Feed data to SageMaker, Databricks |
| Backup | Disaster recovery |
| Data Lake | Centralize for multiple consumers |

---

## 4. File Formats Explained

### CSV (Comma-Separated Values)
```
id,name,amount
1,John,100.50
2,Jane,200.75
```
- ✅ Human-readable, universal
- ❌ Large file size, no schema, slow to query

### Parquet
- **Columnar format** (stores columns together, not rows)
- ✅ Compressed (70-90% smaller), fast for analytics
- ✅ Schema embedded, supports nested data
- ❌ Not human-readable
- **Best for**: Large datasets, analytics workloads

### JSON
```json
{"id": 1, "name": "John", "amount": 100.50}
{"id": 2, "name": "Jane", "amount": 200.75}
```
- ✅ Flexible schema, supports nested data
- ❌ Larger than Parquet, slower to query

### Interview Tip:
> "For analytical workloads, I'd choose **Parquet** because it's columnar - when you query specific columns, it only reads those columns, making it much faster than row-based formats like CSV."

---

## 5. Migration Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     DATA MIGRATION PIPELINE                       │
└──────────────────────────────────────────────────────────────────┘

  S3 → SNOWFLAKE (Ingestion)
  ═══════════════════════════

  ┌─────────┐      ┌─────────────┐      ┌─────────────┐
  │   S3    │ ──── │   Python    │ ──── │  Snowflake  │
  │ (Files) │      │  (Pandas)   │      │  (Tables)   │
  └─────────┘      └─────────────┘      └─────────────┘
       │                 │                     │
   Read file      Transform data         Write to table
   (CSV/Parquet)  (DataFrame)           (write_pandas)


  SNOWFLAKE → S3 (Export)
  ═══════════════════════════

  ┌─────────────┐      ┌─────────────┐      ┌─────────┐
  │  Snowflake  │ ──── │   Python    │ ──── │   S3    │
  │  (Tables)   │      │  (Pandas)   │      │ (Files) │
  └─────────────┘      └─────────────┘      └─────────┘
       │                     │                   │
   Execute SQL         DataFrame           Write file
   (fetch_pandas)      in memory          (put_object)
```

---

## 6. Authentication & Security

### AWS Credentials
```
AWS_ACCESS_KEY_ID     = Your access key (like username)
AWS_SECRET_ACCESS_KEY = Your secret key (like password)
AWS_REGION            = Where your bucket lives
S3_BUCKET_NAME        = Your bucket name
```

### Snowflake Credentials
```
SNOWFLAKE_ACCOUNT   = org-account_name (unique identifier)
SNOWFLAKE_USER      = Username
SNOWFLAKE_PASSWORD  = Password
SNOWFLAKE_WAREHOUSE = Compute cluster name
SNOWFLAKE_DATABASE  = Database name
SNOWFLAKE_SCHEMA    = Schema name (default: PUBLIC)
```

### Security Best Practices (Interview Points):
1. **Never hardcode credentials** - Use environment variables or secrets manager
2. **Use IAM roles** in production (not access keys)
3. **Principle of least privilege** - Grant minimal permissions
4. **Encrypt data in transit** (HTTPS) and at rest (S3 encryption)
5. **Use VPC/Private Link** for Snowflake in production

---

## 7. Key Code Concepts

### Boto3 (AWS SDK for Python)
```python
import boto3

# Create S3 client
client = boto3.client('s3',
    aws_access_key_id='...',
    aws_secret_access_key='...'
)

# Read file
response = client.get_object(Bucket='my-bucket', Key='data/file.csv')

# Write file
client.put_object(Bucket='my-bucket', Key='output/file.csv', Body=data)
```

### Snowflake Connector
```python
import snowflake.connector

# Connect
conn = snowflake.connector.connect(
    account='...', user='...', password='...',
    warehouse='...', database='...', schema='...'
)

# Query to DataFrame
cursor = conn.cursor()
cursor.execute("SELECT * FROM orders")
df = cursor.fetch_pandas_all()

# Write DataFrame to table
from snowflake.connector.pandas_tools import write_pandas
write_pandas(conn, df, 'TABLE_NAME', auto_create_table=True)
```

### Pandas (Data Manipulation)
```python
import pandas as pd

# Read formats
df = pd.read_csv('file.csv')
df = pd.read_parquet('file.parquet')
df = pd.read_json('file.json')

# Write formats
df.to_csv('output.csv', index=False)
df.to_parquet('output.parquet', index=False)
df.to_json('output.json', orient='records')
```

---

## 8. Common Interview Questions

### Q1: "How would you migrate 1TB of data from S3 to Snowflake?"

**Answer:**
> "For large-scale migration, I wouldn't use Python/Pandas due to memory limits. Instead, I'd use **Snowflake's native COPY INTO** command:
>
> 1. Create an **external stage** pointing to S3
> 2. Use `COPY INTO table FROM @stage` - this loads directly without intermediate processing
> 3. Use **file format** specification for CSV/Parquet
> 4. Enable **parallel loading** with multiple files
>
> This is much faster because Snowflake loads directly from S3 using its distributed compute."

```sql
-- Create stage
CREATE STAGE my_s3_stage
  URL = 's3://my-bucket/data/'
  CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...');

-- Load data
COPY INTO my_table
FROM @my_s3_stage
FILE_FORMAT = (TYPE = 'PARQUET');
```

---

### Q2: "How do you handle schema changes during migration?"

**Answer:**
> "Several strategies:
> 1. **auto_create_table=True** - Snowflake infers schema from DataFrame
> 2. **Schema evolution** - Add columns with ALTER TABLE
> 3. **VARIANT column** - Store semi-structured data as JSON
> 4. **Version tables** - Create new tables for breaking changes
> 5. **Schema registry** - Track schema versions in a metadata table"

---

### Q3: "How would you schedule this pipeline in production?"

**Answer:**
> "Several options depending on the stack:
> - **Airflow** - Most common, DAG-based orchestration
> - **AWS Step Functions** - Native AWS serverless
> - **Snowflake Tasks** - Built-in scheduling
> - **dbt** - For transformation-focused pipelines
> - **Cron** - Simple, for basic needs
>
> I'd choose **Airflow** for complex dependencies, **Snowflake Tasks** for Snowflake-centric workloads."

---

### Q4: "What's the difference between ETL and ELT?"

**Answer:**
> | ETL | ELT |
> |-----|-----|
> | Extract → Transform → Load | Extract → Load → Transform |
> | Transform before loading | Transform in warehouse |
> | Limited by ETL tool compute | Uses warehouse compute (scalable) |
> | Good for small data | Better for big data |
>
> "Modern cloud warehouses like Snowflake favor **ELT** because transformation in-warehouse is faster and more scalable."

---

### Q5: "How do you ensure data quality during migration?"

**Answer:**
> "I implement multiple checks:
> 1. **Row count validation** - Source vs destination
> 2. **Checksum/hash** - Verify data integrity
> 3. **Schema validation** - Column types match
> 4. **Null checks** - Required fields populated
> 5. **Sample comparison** - Random rows match
> 6. **Automated tests** - Great Expectations, dbt tests"

---

### Q6: "What's idempotency and why is it important?"

**Answer:**
> "Idempotency means running the same operation multiple times produces the same result. For data pipelines:
> - If pipeline fails and reruns, it shouldn't duplicate data
> - Implement with: `overwrite=True`, `MERGE` statements, or delete-then-insert
> - Critical for reliability - pipelines will fail and need reruns"

---

## 9. Project Structure Explained

```
data-migration/
├── src/
│   ├── config.py              # Loads credentials from .env
│   ├── s3_connector.py        # S3 read/write operations
│   ├── snowflake_connector.py # Snowflake connection & queries
│   ├── migrate.py             # Single table migration
│   └── pipeline.py            # Batch pipeline with config
├── config/
│   └── export_tables.yaml     # Pipeline configuration
├── .env                       # Credentials (never commit!)
├── .env.example               # Template for credentials
├── requirements.txt           # Python dependencies
└── venv/                      # Virtual environment
```

---

## 10. Key Terms Glossary

| Term | Definition |
|------|------------|
| **Data Lake** | Raw data storage (S3) - schema on read |
| **Data Warehouse** | Structured data for analytics (Snowflake) - schema on write |
| **ETL** | Extract, Transform, Load |
| **ELT** | Extract, Load, Transform |
| **Stage** | Snowflake's reference to external storage |
| **Warehouse** | Snowflake compute cluster |
| **Columnar Storage** | Data stored by column (Parquet, Snowflake) |
| **Idempotent** | Same result regardless of how many times executed |
| **Data Pipeline** | Automated workflow moving data from A to B |
| **Schema** | Structure definition (columns, types) |

---

## 11. What to Say in Your Interview

### Opening Statement:
> "I built a Python-based data migration pipeline that transfers data bidirectionally between AWS S3 and Snowflake. It supports multiple file formats - CSV for compatibility, Parquet for performance, and JSON for semi-structured data."

### Technical Deep Dive:
> "The architecture uses boto3 for S3 operations, the Snowflake connector for warehouse interactions, and Pandas for data transformation. I implemented a configuration-driven pipeline that reads from a YAML file, allowing non-technical users to add new tables without code changes."

### Production Considerations:
> "For production, I'd enhance this with proper error handling, retry logic, data validation checks, and integrate with Airflow for orchestration. For very large datasets, I'd switch to Snowflake's native COPY INTO command which leverages distributed loading."

---

Good luck with your interview! 🎯
