"""
DAG для работы с MSSQL: выборка команд → валидация и загрузка по каждой.

Шаг 1: выборка номера команды (KOM_NUMBER) и даты команды — одна или несколько строк.
Шаг 2: для каждой строки по очереди:
  - dbo.COMMANDS_VALIDATION(KOM_NUMBER)
  - dbo.FIND_DUBLS(KOM_NUMBER)
  - dbo.LOAD_PRIZNAKI(KOM_NUMBER)

Зависимость: apache-airflow-providers-microsoft-mssql
Connection: в Airflow Admin → Connections создайте conn_id (по умолчанию mssql_default).
SQL выборки: замените GET_COMMANDS_SQL на свой запрос (таблица, фильтры).
Если имена параметров процедур другие (например @KomNumber) — измените в validate_and_load.
"""

import pendulum
from airflow.sdk import DAG, task

# Тяжёлые импорты только внутри задач (Best Practices)
# conn_id для MSSQL — из Airflow Connections
CONN_ID = "mssql_default"

# Запрос выборки: номер команды и дата. Замените YOUR_TABLE и условия на свои.
# Плейсхолдеры %s — под (start_date, end_date) для идемпотентности по интервалу.
# Вариант без дат: SELECT KOM_NUMBER, COMMAND_DATE FROM dbo.YOUR_TABLE ...
GET_COMMANDS_SQL = """
SELECT KOM_NUMBER, COMMAND_DATE
FROM dbo.YOUR_TABLE
WHERE COMMAND_DATE >= CAST(%s AS DATE)
  AND COMMAND_DATE <  CAST(%s AS DATE)
ORDER BY KOM_NUMBER
"""


@task
def get_commands():
    """
    Шаг 1: выборка KOM_NUMBER и даты команды.
    Возвращает список словарей [{"KOM_NUMBER": ..., "COMMAND_DATE": ...}, ...].
    """
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from airflow.sdk import get_current_context

    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    data_interval_start = dag_run.data_interval_start if dag_run else None
    data_interval_end = dag_run.data_interval_end if dag_run else None
    # Партиция по интервалу (идемпотентность)
    start_dt = data_interval_start.strftime("%Y-%m-%d") if data_interval_start else None
    end_dt = data_interval_end.strftime("%Y-%m-%d") if data_interval_end else None

    hook = MsSqlHook(mssql_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(GET_COMMANDS_SQL, (start_dt, end_dt))
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]
        # Сериализуем даты в строки для XCom
        for r in result:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return result
    finally:
        cursor.close()
        conn.close()


@task
def validate_and_load(command: dict):
    """
    Шаг 2: для одной команды — COMMANDS_VALIDATION → FIND_DUBLS → LOAD_PRIZNAKI.
    Вход: один элемент из результата get_commands (KOM_NUMBER, COMMAND_DATE).
    """
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

    kom_number = command.get("KOM_NUMBER")
    if kom_number is None:
        raise ValueError("KOM_NUMBER отсутствует в записи")

    hook = MsSqlHook(mssql_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    procedures = [
        ("dbo.COMMANDS_VALIDATION", "COMMANDS_VALIDATION"),
        ("dbo.FIND_DUBLS", "FIND_DUBLS"),
        ("dbo.LOAD_PRIZNAKI", "LOAD_PRIZNAKI"),
    ]

    try:
        for proc_name, short_name in procedures:
            sql = f"EXEC {proc_name} @KOM_NUMBER = %s"
            cursor.execute(sql, (kom_number,))
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"KOM_NUMBER={kom_number}, процедура {proc_name}: {e}") from e
    finally:
        cursor.close()
        conn.close()

    return {"KOM_NUMBER": kom_number, "status": "ok"}


with DAG(
    dag_id="mssql_commands_validation",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mssql", "validation", "commands"],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
        "owner": "airflow",
    },
) as dag:
    commands = get_commands()
    validate_and_load.expand(command=commands)
