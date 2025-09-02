from __future__ import annotations
import os, pendulum, datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import yfinance as yf
import great_expectations as ge
from utils.io import s3_path, write_parquet_df, read_parquet_df

DEFAULT_ARGS = {
    "owner": "alpha",
    "retries": 1,
}

TICKERS = ["AAPL", "MSFT", "AMZN"]

def fetch_eod(**context):
    # Fetch last 5 business days to be resilient
    end = dt.date.today()
    start = end - dt.timedelta(days=10)
    frames = []
    for t in TICKERS:
        df = yf.download(t, start=start.isoformat(), end=end.isoformat(), auto_adjust=False, progress=False)
        if df.empty:
            continue
        df = df.reset_index().rename(columns=str.lower)
        df["ticker"] = t
        frames.append(df[["date","ticker","open","high","low","close","adj close","volume"]])
    if not frames:
        raise ValueError("No data fetched")
    all_df = pd.concat(frames, ignore_index=True)
    ds = end.isoformat()
    out = s3_path("bronze","equities", f"dt={ds}", "eod.parquet")
    write_parquet_df(all_df, out)
    return out

def validate_eod(**context):
    path = context["ti"].xcom_pull(task_ids="fetch_eod")
    df = read_parquet_df(path)
    gdf = ge.from_pandas(df)
    # Basic checks
    gdf.expect_column_values_to_not_be_null("close")
    gdf.expect_column_values_to_be_between("volume", min_value=0)
    gdf.expect_column_values_to_be_of_type("ticker", "str")
    res = gdf.validate()
    if not res["success"]:
        raise ValueError("GE validation failed")
    return path

def promote_to_silver(**context):
    path = context["ti"].xcom_pull(task_ids="validate_eod")
    df = read_parquet_df(path)
    df = df.rename(columns={"adj close":"adj_close"})
    df["return_1d"] = df.groupby("ticker")["adj_close"].pct_change()
    ds = df["date"].max().date().isoformat() if hasattr(df["date"].max(), "date") else str(df["date"].max())
    out = s3_path("silver","equities", f"dt={ds}", "eod_features.parquet")
    write_parquet_df(df, out)
    return out

with DAG(
    dag_id="eod_ingest_and_promote",
    default_args=DEFAULT_ARGS,
    schedule="0 23 * * 1-5",
    start_date=pendulum.datetime(2024,1,1, tz="Europe/Paris"),
    catchup=False,
    tags=["eod","silver","quality"],
) as dag:

    t1 = PythonOperator(task_id="fetch_eod", python_callable=fetch_eod)
    t2 = PythonOperator(task_id="validate_eod", python_callable=validate_eod)
    t3 = PythonOperator(task_id="promote_to_silver", python_callable=promote_to_silver)

    t1 >> t2 >> t3
