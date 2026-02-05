"""
DAG для работы с MSSQL: выборка команд → валидация и загрузка по каждой.

Итерация строго по номеру команды (KOM_NUMBER): на вход каждой процедуры подаётся KOM_NUMBER.
Каждая процедура запускается в отдельной задаче Airflow = отдельное подключение к MSSQL,
что устраняет проблемы с выполнением двух транзакций подряд в одной сессии.

Цепочка для каждого KOM_NUMBER:
  get_commands → run_commands_validation → run_find_dubls → run_load_priznaki

Зависимость: apache-airflow-providers-microsoft-mssql
Connection: mssql_default. SQL и имена процедур — настраиваются ниже.
"""

import pendulum
from airflow.sdk import DAG, task

CONN_ID = "mssql_default"

# Запрос выборки: только KOM_NUMBER (и при необходимости дата для фильтра).
# Итерация в DAG идёт по списку KOM_NUMBER.
GET_COMMANDS_SQL = """
SELECT KOM_NUMBER, COMMAND_DATE
FROM dbo.YOUR_TABLE
WHERE COMMAND_DATE >= CAST(%s AS DATE)
  AND COMMAND_DATE <  CAST(%s AS DATE)
ORDER BY KOM_NUMBER,
"""

# Имена процедур и параметр на вход — для гибкости (можно вынести в Variable).
PROC_COMMANDS_VALIDATION = "dbo.COMMANDS_VALIDATION"
PROC_FIND_DUBLS = "dbo.FIND_DUBLS"
PROC_LOAD_PRIZNAKI = "dbo.LOAD_PRIZNAKI"
PROC_PARAM_NAME = "KOM_NUMBER"


def _run_single_procedure(conn_id: str, proc_name: str, kom_number) -> None:
    """
    Одна процедура в одном подключении: открыли → EXEC → commit → закрыли.
    Избегаем проблем MSSQL с двумя транзакциями подряд в одной сессии.
    """
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

    hook = MsSqlHook(mssql_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        sql = f"EXEC {proc_name} @{PROC_PARAM_NAME} = %s"
        cursor.execute(sql, (kom_number,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"KOM_NUMBER={kom_number}, {proc_name}: {e}") from e
    finally:
        cursor.close()
        conn.close()


@task
def get_commands():
    """
    Шаг 1: выборка по интервалу. Возвращает список записей с KOM_NUMBER.
    Итерация в DAG идёт по этому списку — по одному KOM_NUMBER на вход процедур.
    """
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from airflow.sdk import get_current_context

    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    data_interval_start = dag_run.data_interval_start if dag_run else None
    data_interval_end = dag_run.data_interval_end if dag_run else None
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
        for r in result:
            for k, v in list(r.items()):
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return result
    finally:
        cursor.close()
        conn.close()


@task
def run_commands_validation(command: dict):
    """Одна задача = одно подключение. Вызов только COMMANDS_VALIDATION(KOM_NUMBER)."""
    kom_number = command.get("KOM_NUMBER")
    if kom_number is None:
        raise ValueError("KOM_NUMBER отсутствует в записи")
    _run_single_procedure(CONN_ID, PROC_COMMANDS_VALIDATION, kom_number)
    return {"KOM_NUMBER": kom_number, "step": "COMMANDS_VALIDATION"}


@task
def run_find_dubls(command: dict):
    """Отдельное подключение. Вызов только FIND_DUBLS(KOM_NUMBER)."""
    kom_number = command.get("KOM_NUMBER")
    if kom_number is None:
        raise ValueError("KOM_NUMBER отсутствует в записи")
    _run_single_procedure(CONN_ID, PROC_FIND_DUBLS, kom_number)
    return {"KOM_NUMBER": kom_number, "step": "FIND_DUBLS"}


@task
def run_load_priznaki(command: dict):
    """Отдельное подключение. Вызов только LOAD_PRIZNAKI(KOM_NUMBER)."""
    kom_number = command.get("KOM_NUMBER")
    if kom_number is None:
        raise ValueError("KOM_NUMBER отсутствует в записи")
    _run_single_procedure(CONN_ID, PROC_LOAD_PRIZNAKI, kom_number)
    return {"KOM_NUMBER": kom_number, "step": "LOAD_PRIZNAKI"}


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

    # Итерация по номеру команды: каждая процедура — отдельная задача (отдельная сессия MSSQL).
    validation = run_commands_validation.expand(command=commands)
    find_dubls = run_find_dubls.expand(command=commands)
    load_priznaki = run_load_priznaki.expand(command=commands)

    
