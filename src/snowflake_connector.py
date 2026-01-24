import snowflake.connector
import pandas as pd
from config import SnowflakeConfig


class SnowflakeConnector:
    def __init__(self):
        self.connection = None

    def connect(self):
        self.connection = snowflake.connector.connect(
            account=SnowflakeConfig.ACCOUNT,
            user=SnowflakeConfig.USER,
            password=SnowflakeConfig.PASSWORD,
            warehouse=SnowflakeConfig.WAREHOUSE,
            database=SnowflakeConfig.DATABASE,
            schema=SnowflakeConfig.SCHEMA,
        )
        return self.connection

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def execute_query(self, query: str):
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor

    def fetch_dataframe(self, query: str) -> pd.DataFrame:
        cursor = self.connection.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        cursor.close()
        return df

    def write_dataframe(self, df: pd.DataFrame, table_name: str, overwrite: bool = False):
        from snowflake.connector.pandas_tools import write_pandas

        write_pandas(
            self.connection,
            df,
            table_name,
            auto_create_table=True,
            overwrite=overwrite,
        )

    def create_table_from_df(self, df: pd.DataFrame, table_name: str):
        from snowflake.connector.pandas_tools import write_pandas

        write_pandas(
            self.connection,
            df,
            table_name,
            auto_create_table=True,
            overwrite=True,
        )
