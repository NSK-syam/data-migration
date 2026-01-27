import yaml
import logging
from pathlib import Path
from snowflake_connector import SnowflakeConnector
from s3_exporter import S3Exporter

PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "export_tables.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config(config_path: str | Path = None) -> dict:
    if config_path is None:
        config_path = DEFAULT_CONFIG
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_export(export_config: dict, snowflake: SnowflakeConnector, s3: S3Exporter) -> str:
    table = export_config["table"]
    s3_path = export_config["s3_path"]
    file_type = export_config.get("file_type", "parquet")
    query = export_config.get("query")

    if query is None:
        query = f"SELECT * FROM {table}"

    logger.info(f"Fetching data for {table}")
    df = snowflake.fetch_dataframe(query)
    logger.info(f"Fetched {len(df)} rows")

    logger.info(f"Exporting to {s3_path}")
    result = s3.export_dataframe(df, s3_path, file_type)
    logger.info(f"Export complete: {result}")

    return result


def run_pipeline(config_path: str | Path = None):
    config = load_config(config_path)
    exports = config.get("exports", [])

    if not exports:
        logger.warning("No exports configured")
        return

    snowflake = SnowflakeConnector()
    s3 = S3Exporter()

    try:
        logger.info("Connecting to Snowflake")
        snowflake.connect()

        results = []
        for export_config in exports:
            try:
                result = run_export(export_config, snowflake, s3)
                results.append({"table": export_config["table"], "status": "success", "path": result})
            except Exception as e:
                logger.error(f"Failed to export {export_config['table']}: {e}")
                results.append({"table": export_config["table"], "status": "failed", "error": str(e)})

        logger.info("Pipeline complete")
        for r in results:
            status = "✓" if r["status"] == "success" else "✗"
            logger.info(f"  {status} {r['table']}")

        return results

    finally:
        snowflake.disconnect()


if __name__ == "__main__":
    run_pipeline()
