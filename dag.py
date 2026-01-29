from future import annotations

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


# --- Конфигурация ---
DAG_ID = "kom_mssql_pipeline"
MSSQL_CONN_ID = "mssql_kom"  # Airflow Connection (conn_type=mssql)

# Ваш SELECT: важно, чтобы он возвращал KOM_NUMBER, KOM_DATE
KOM_SELECT_SQL = """
SELECT
    KOM_NUMBER,
    KOM_DATE
FROM dbo.KOM_SOURCE
-- Рекомендуется отбирать только "необработанные" записи
-- WHERE IsProcessed = 0
ORDER BY KOM_DATE
"""

# Имена хранимых процедур (пример)
PROC_VALIDATE = "dbo.usp_kom_validate"
PROC_DEDUP    = "dbo.usp_kom_find_duplicates"
PROC_INSERT   = "dbo.usp_kom_insert"
PROC_DELETE   = "dbo.usp_kom_delete"


def _call_proc(proc_name: str, kom_number: str) -> None:
    """
    Унифицированный вызов процедуры в MSSQL через MsSqlHook (pymssql).
    """
    hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    conn = hook.get_conn()  # pymssql connection :contentReference[oaicite:1]{index=1}
    try:
        hook.set_autocommit(conn, True)  # supports_autocommit :contentReference[oaicite:2]{index=2}
        cur = conn.cursor()
        try:
            # callproc безопаснее, чем вручную собирать EXEC с плейсхолдерами
            cur.callproc(proc_name, (kom_number,))
        finally:
            cur.close()
    finally:
        conn.close()


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Belgrade"),
    schedule="0 * * * *",          # каждый час
    catchup=False,
    max_active_runs=1,             # следующий запуск подождёт завершения текущего
    dagrun_timeout=timedelta(hours=12),
    default_args={
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    # Ограничьте параллелизм, чтобы не “положить” MSSQL при большом числе KOM_NUMBER
    max_active_tasks=16,
    tags=["mssql", "kom"],
) as dag:

    @task
    def fetch_kom_records() -> list[dict]:
        """
        Возвращает список объектов вида:
        [{"kom_number": "...", "kom_date": "YYYY-MM-DDTHH:mm:ss"}, ...]
        """
        hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        rows = hook.get_records(KOM_SELECT_SQL)  # common.sql hook API :contentReference[oaicite:3]{index=3}

        result: list[dict] = []
        for kom_number, kom_date in rows:
            result.append(
                {
                    "kom_number": str(kom_number),
                    "kom_date": kom_date.isoformat() if hasattr(kom_date, "isoformat") else str(kom_date),
                }
            )
        return result

    @task
    def validate(record: dict) -> dict:
        _call_proc(PROC_VALIDATE, record["kom_number"])
        return record

    @task
    def find_duplicates(record: dict) -> dict:
        _call_proc(PROC_DEDUP, record["kom_number"])
        return record

    @task
    def do_insert(record: dict) -> dict:
        _call_proc(PROC_INSERT, record["kom_number"])
        return record

    @task
    def do_delete(record: dict) -> dict:
        _call_proc(PROC_DELETE, record["kom_number"])
        return record

    # 1) SELECT -> 2) маппинг по записям (dynamic task mapping) :contentReference[oaicite:4]{index=4}
    records = fetch_kom_records()

    validated = validate.expand(record=records)
    deduped = find_duplicates.expand(record=validated)
    inserted = do_insert.expand(record=deduped)
    do_delete.expand(record=inserted)
