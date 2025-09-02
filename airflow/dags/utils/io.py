import os
import io
import pandas as pd

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "alpha")

def s3_path(*parts):
    key = "/".join(p.strip("/") for p in parts)
    return f"s3://{S3_BUCKET}/{key}"

def write_parquet_df(df: pd.DataFrame, path: str):
    storage_options = {
        "key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "client_kwargs": {"endpoint_url": S3_ENDPOINT_URL},
    }
    df.to_parquet(path, index=False, storage_options=storage_options)

def read_parquet_df(path: str) -> pd.DataFrame:
    storage_options = {
        "key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "client_kwargs": {"endpoint_url": S3_ENDPOINT_URL},
    }
    return pd.read_parquet(path, storage_options=storage_options)
