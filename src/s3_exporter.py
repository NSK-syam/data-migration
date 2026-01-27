import io
import boto3
import pandas as pd
from config import AWSConfig


class S3Exporter:
    def __init__(self):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
            aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY,
            region_name=AWSConfig.REGION,
        )
        self.bucket = AWSConfig.S3_BUCKET_NAME

    def export_dataframe(
        self, df: pd.DataFrame, s3_path: str, file_type: str = "parquet"
    ) -> str:
        file_type = file_type.lower()
        buffer = io.BytesIO()

        if file_type == "parquet":
            df.to_parquet(buffer, index=False)
            content_type = "application/octet-stream"
        elif file_type == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            buffer = io.BytesIO(buffer.getvalue().encode("utf-8"))
            content_type = "text/csv"
        elif file_type == "json":
            buffer = io.StringIO()
            df.to_json(buffer, orient="records", lines=True)
            buffer = io.BytesIO(buffer.getvalue().encode("utf-8"))
            content_type = "application/json"
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        buffer.seek(0)
        self.client.put_object(
            Bucket=self.bucket,
            Key=s3_path,
            Body=buffer.getvalue(),
            ContentType=content_type,
        )

        return f"s3://{self.bucket}/{s3_path}"

    def file_exists(self, s3_path: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=s3_path)
            return True
        except self.client.exceptions.ClientError:
            return False

    def delete_file(self, s3_path: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=s3_path)

    def list_files(self, prefix: str = "") -> list[str]:
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        if "Contents" not in response:
            return []
        return [obj["Key"] for obj in response["Contents"]]
