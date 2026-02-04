"""
DAG для S3-совместимого хранилища (AWS S3 или SeaweedFS).

List, upload, download через S3Hook. SeaweedFS поддерживает S3 API — задайте endpoint_url в Connection.
Зависимость: apache-airflow-providers-amazon
Connection: s3_default или seaweedfs (Extra: {"endpoint_url": "http://seaweedfs:8333"} для SeaweedFS).
"""

import pendulum
from airflow.sdk import DAG, task


@task
def s3_list_keys():
    """Список ключей в бакете (префикс по партиции)."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.sdk import get_current_context

    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    prefix = "raw/"
    if dag_run and dag_run.data_interval_start:
        prefix = f"raw/{dag_run.data_interval_start.strftime('%Y-%m-%d')}/"

    hook = S3Hook(aws_conn_id="s3_default")
    bucket = "my-bucket"
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
    return {"bucket": bucket, "prefix": prefix, "keys": keys or [], "count": len(keys or [])}


@task
def s3_upload_sample(list_result: dict):
    """Пример загрузки файла в S3/SeaweedFS."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id="s3_default")
    bucket = list_result.get("bucket", "my-bucket")
    prefix = list_result.get("prefix", "raw/")
    key = f"{prefix}sample_{pendulum.now('UTC').int_timestamp}.txt"
    content = "Hello from Airflow S3/SeaweedFS"
    hook.load_string(content, key=key, bucket_name=bucket, replace=True)
    return {"uploaded": key}


with DAG(
    dag_id="s3_seaweedfs_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["s3", "seaweedfs", "object-storage"],
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1), "owner": "airflow"},
) as dag:
    listed = s3_list_keys()
    s3_upload_sample(listed)
