import yaml
import argparse
from datetime import datetime
from s3_connector import S3Connector
from snowflake_connector import SnowflakeConnector


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_pipeline(config_path: str, dry_run: bool = False):
    config = load_config(config_path)
    exports = config.get("exports", [])

    if not exports:
        print("No exports defined in config")
        return

    print(f"Pipeline started at {datetime.now().isoformat()}")
    print(f"Found {len(exports)} table(s) to export")
    print("-" * 50)

    s3 = S3Connector()
    snowflake = SnowflakeConnector()
    snowflake.connect()

    results = []

    for export in exports:
        table = export["table"]
        s3_path = export["s3_path"]
        file_type = export.get("file_type", "csv")
        query = export.get("query") or f"SELECT * FROM {table}"

        print(f"\nExporting: {table}")
        print(f"  Query: {query[:80]}{'...' if len(query) > 80 else ''}")
        print(f"  Destination: s3://{s3.bucket}/{s3_path}")
        print(f"  Format: {file_type}")

        if dry_run:
            print("  [DRY RUN] Skipping actual export")
            results.append({"table": table, "status": "skipped (dry run)"})
            continue

        try:
            df = snowflake.fetch_dataframe(query)
            row_count = len(df)

            if file_type == "csv":
                s3.write_csv(df, s3_path)
            elif file_type == "parquet":
                s3.write_parquet(df, s3_path)
            elif file_type == "json":
                s3.write_json(df, s3_path)

            print(f"  Exported {row_count} rows")
            results.append({"table": table, "status": "success", "rows": row_count})

        except Exception as e:
            print(f"  ERROR: {e}")
            results.append({"table": table, "status": "failed", "error": str(e)})

    snowflake.disconnect()

    print("\n" + "=" * 50)
    print("Pipeline Summary")
    print("=" * 50)
    for r in results:
        status = r["status"]
        rows = r.get("rows", "-")
        print(f"  {r['table']}: {status} ({rows} rows)")

    print(f"\nPipeline completed at {datetime.now().isoformat()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Snowflake to S3 export pipeline")
    parser.add_argument("--config", default="config/export_tables.yaml",
                        help="Path to export config file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be exported without actually exporting")

    args = parser.parse_args()
    run_pipeline(args.config, args.dry_run)
