"""
DAG для ClickHouse.

Выполнение SQL (SELECT/INSERT) через ClickHouse Hook/Operator.
Зависимость: apache-airflow-providers-clickhouse или airflow-providers-clickhouse
Connection: clickhouse_default (тип: generic с host/port или clickhouse).
"""

import pendulum
from airflow.sdk import DAG, task


@task
def clickhouse_select():
    """Выборка из ClickHouse по партиции."""
    from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
    from airflow.sdk import get_current_context

    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    partition = "unknown"
    if dag_run and dag_run.data_interval_start:
        partition = dag_run.data_interval_start.strftime("%Y-%m-%d")

    hook = ClickHouseHook(clickhouse_conn_id="clickhouse_default")
    sql = "SELECT count() AS cnt FROM system.tables WHERE database = currentDatabase()"
    result = hook.get_records(sql)
    count = result[0][0] if result else 0
    return {"partition": partition, "tables_count": count}


@task
def clickhouse_insert(select_result: dict):
    """Пример записи в таблицу (подставьте свою таблицу и поля)."""
    from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook

    hook = ClickHouseHook(clickhouse_conn_id="clickhouse_default")
    partition = select_result.get("partition", "unknown")
    sql = """
    INSERT INTO default.airflow_log (partition_date, metric_name, metric_value, created_at)
    VALUES ('{partition}', 'tables_count', {value}, now())
    """.format(
        partition=partition.replace("'", "''"),
        value=select_result.get("tables_count", 0),
    )
    hook.run(sql)
    return f"Inserted partition={partition}"


with DAG(
    dag_id="clickhouse_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["clickhouse", "analytics"],
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1), "owner": "airflow"},
) as dag:
    data = clickhouse_select()
    clickhouse_insert(data)
