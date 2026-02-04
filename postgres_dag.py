"""
DAG для работы с PostgreSQL.

Примеры: выборка данных по партиции, запись в таблицу (UPSERT), использование Hook.
Зависимость: apache-airflow-providers-postgres
Connection: postgres_default (Host, Login, Password, Schema, Port).
"""

import pendulum
from airflow.sdk import DAG, task


@task
def postgres_select():
    """Выборка из PostgreSQL по интервалу (идемпотентность)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.sdk import get_current_context

    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    partition = "unknown"
    if dag_run and dag_run.data_interval_start:
        partition = dag_run.data_interval_start.strftime("%Y-%m-%d")

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT id, name, created_at FROM example_table WHERE created_at::date = %s LIMIT 10",
            (partition,),
        )
        rows = cursor.fetchall()
        columns = [col.name for col in cursor.description]
        result = [dict(zip(columns, r)) for r in rows]
        for r in result:
            for k, v in list(r.items()):
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return {"partition": partition, "count": len(result), "rows": result}
    finally:
        cursor.close()
        conn.close()


@task
def postgres_upsert(select_result: dict):
    """Пример UPSERT в другую таблицу (не INSERT — повторный запуск без дубликатов)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
    INSERT INTO example_summary (partition_date, row_count, updated_at)
    VALUES (%s, %s, NOW())
    ON CONFLICT (partition_date) DO UPDATE SET row_count = EXCLUDED.row_count, updated_at = NOW()
    """
    partition = select_result.get("partition", "unknown")
    count = select_result.get("count", 0)
    hook.run(sql, parameters=(partition, count))
    return f"Upserted partition={partition} count={count}"


with DAG(
    dag_id="postgres_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["postgres", "etl"],
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1), "owner": "airflow"},
) as dag:
    data = postgres_select()
    postgres_upsert(data)
