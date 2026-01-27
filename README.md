# Data Migration

A Python-based data pipeline for exporting data from Snowflake to AWS S3.

## Features

- Connect to Snowflake and execute queries
- Export tables to S3 in multiple formats (Parquet, CSV, JSON)
- Configuration-driven exports via YAML
- Support for custom SQL queries

## Prerequisites

- Python 3.14+
- AWS account with S3 access
- Snowflake account

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd data-migration
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment variables:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` with your credentials.

## Configuration

### Environment Variables

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_REGION` | AWS region (default: us-east-1) |
| `S3_BUCKET_NAME` | Target S3 bucket |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse |
| `SNOWFLAKE_DATABASE` | Snowflake database |
| `SNOWFLAKE_SCHEMA` | Snowflake schema |

### Export Configuration

Define tables to export in `config/export_tables.yaml`:

```yaml
exports:
  - table: ORDERS
    s3_path: exports/orders/data.parquet
    file_type: parquet
    query: null  # null = export full table

  - table: RECENT_ORDERS
    s3_path: exports/recent_orders/data.parquet
    file_type: parquet
    query: "SELECT * FROM ORDERS WHERE created_at >= DATEADD(day, -7, CURRENT_DATE())"
```

## Usage

Run the pipeline:

```bash
cd src
python pipeline.py
```

Or with a custom config file:

```python
from pipeline import run_pipeline
run_pipeline("path/to/custom_config.yaml")
```

## Testing

Test with sample data (no Snowflake connection required):

```bash
cd src

# Generate local test files (parquet, csv)
python test_pipeline.py

# Test S3 upload (requires AWS credentials in .env)
python test_pipeline.py --s3
```

## Project Structure

```
data-migration/
├── config/
│   └── export_tables.yaml    # Export configuration
├── src/
│   ├── config.py             # Environment configuration
│   ├── snowflake_connector.py # Snowflake connection handler
│   ├── s3_exporter.py        # S3 upload handler
│   ├── pipeline.py           # Main pipeline orchestrator
│   └── test_pipeline.py      # Test with sample data
├── .env.example              # Environment template
├── requirements.txt          # Python dependencies
└── README.md
```

## Dependencies

- `boto3` - AWS SDK for Python
- `snowflake-connector-python` - Snowflake connector
- `pandas` - Data manipulation
- `pyarrow` - Parquet file support
- `python-dotenv` - Environment variable management
- `pyyaml` - YAML configuration parsing