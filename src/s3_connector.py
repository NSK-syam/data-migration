import boto3
import pandas as pd
from io import BytesIO
from config import AWSConfig


class S3Connector:
    def __init__(self):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
            aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY,
            region_name=AWSConfig.REGION,
        )
        self.bucket = AWSConfig.S3_BUCKET_NAME

    def list_files(self, prefix: str = "") -> list:
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]

    def read_csv(self, key: str) -> pd.DataFrame:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return pd.read_csv(BytesIO(response["Body"].read()))

    def read_parquet(self, key: str) -> pd.DataFrame:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return pd.read_parquet(BytesIO(response["Body"].read()))

    def read_json(self, key: str) -> pd.DataFrame:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return pd.read_json(BytesIO(response["Body"].read()))

    def download_file(self, key: str, local_path: str):
        self.client.download_file(self.bucket, key, local_path)

    def write_csv(self, df: pd.DataFrame, key: str):
        buffer = BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())

    def write_parquet(self, df: pd.DataFrame, key: str):
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())

    def write_json(self, df: pd.DataFrame, key: str):
        buffer = BytesIO()
        df.to_json(buffer, orient="records", lines=True)
        buffer.seek(0)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
