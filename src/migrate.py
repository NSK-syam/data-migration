import argparse
from s3_connector import S3Connector
from snowflake_connector import SnowflakeConnector


def migrate_s3_to_snowflake(s3_key: str, table_name: str, file_type: str = "csv"):
    s3 = S3Connector()
    snowflake = SnowflakeConnector()

    print(f"Reading {file_type} file from S3: {s3_key}")
    if file_type == "csv":
        df = s3.read_csv(s3_key)
    elif file_type == "parquet":
        df = s3.read_parquet(s3_key)
    elif file_type == "json":
        df = s3.read_json(s3_key)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    print(f"Loaded {len(df)} rows from S3")

    print(f"Connecting to Snowflake...")
    snowflake.connect()

    print(f"Writing data to Snowflake table: {table_name}")
    snowflake.write_dataframe(df, table_name, overwrite=True)

    print(f"Migration complete. {len(df)} rows written to {table_name}")
    snowflake.disconnect()


def migrate_snowflake_to_s3(query: str, s3_key: str, file_type: str = "csv"):
    """Migrate data from Snowflake query results to S3."""
    s3 = S3Connector()
    snowflake = SnowflakeConnector()

    print(f"Connecting to Snowflake...")
    snowflake.connect()

    print(f"Executing query...")
    df = snowflake.fetch_dataframe(query)
    print(f"Fetched {len(df)} rows from Snowflake")

    print(f"Writing {file_type} to S3: {s3_key}")
    if file_type == "csv":
        s3.write_csv(df, s3_key)
    elif file_type == "parquet":
        s3.write_parquet(df, s3_key)
    elif file_type == "json":
        s3.write_json(df, s3_key)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    snowflake.disconnect()
    print(f"Migration complete. {len(df)} rows written to s3://{s3.bucket}/{s3_key}")


def migrate_table_to_s3(table_name: str, s3_key: str, file_type: str = "csv"):
    """Migrate an entire Snowflake table to S3."""
    query = f"SELECT * FROM {table_name}"
    migrate_snowflake_to_s3(query, s3_key, file_type)


def migrate_batch(prefix: str, table_prefix: str, file_type: str = "csv"):
    s3 = S3Connector()
    snowflake = SnowflakeConnector()

    files = s3.list_files(prefix)
    print(f"Found {len(files)} files with prefix: {prefix}")

    snowflake.connect()

    for file_key in files:
        if not file_key.endswith(f".{file_type}"):
            continue

        table_name = f"{table_prefix}_{file_key.split('/')[-1].replace(f'.{file_type}', '')}"
        print(f"Migrating {file_key} -> {table_name}")

        if file_type == "csv":
            df = s3.read_csv(file_key)
        elif file_type == "parquet":
            df = s3.read_parquet(file_key)
        elif file_type == "json":
            df = s3.read_json(file_key)

        snowflake.write_dataframe(df, table_name.upper(), overwrite=True)
        print(f"  Written {len(df)} rows")

    snowflake.disconnect()
    print("Batch migration complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate data between AWS S3 and Snowflake")
    parser.add_argument("--direction", choices=["s3-to-snowflake", "snowflake-to-s3"],
                        default="s3-to-snowflake", help="Migration direction")
    parser.add_argument("--s3-key", help="S3 object key (source or destination)")
    parser.add_argument("--table", help="Snowflake table name")
    parser.add_argument("--query", help="SQL query for Snowflake export (snowflake-to-s3)")
    parser.add_argument("--file-type", default="csv", choices=["csv", "parquet", "json"])
    parser.add_argument("--batch-prefix", help="S3 prefix for batch migration (s3-to-snowflake)")
    parser.add_argument("--table-prefix", help="Table name prefix for batch migration")

    args = parser.parse_args()

    if args.direction == "snowflake-to-s3":
        if args.query and args.s3_key:
            migrate_snowflake_to_s3(args.query, args.s3_key, args.file_type)
        elif args.table and args.s3_key:
            migrate_table_to_s3(args.table, args.s3_key, args.file_type)
        else:
            print("Error: snowflake-to-s3 requires --s3-key and either --table or --query")
            parser.print_help()
    elif args.direction == "s3-to-snowflake":
        if args.batch_prefix:
            migrate_batch(args.batch_prefix, args.table_prefix or "migrated", args.file_type)
        elif args.s3_key and args.table:
            migrate_s3_to_snowflake(args.s3_key, args.table, args.file_type)
        else:
            print("Error: s3-to-snowflake requires --s3-key and --table, or --batch-prefix")
            parser.print_help()
