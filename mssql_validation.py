"""
DAG: mssql_commands_validation
Выборка команд из MSSQL → последовательная обработка каждой тремя процедурами.

Порядок:
  1) get_commands — SELECT KOM_NUMBER по интервалу (data_interval_start..end)
  2) process_commands — для каждого KOM_NUMBER строго по очереди:
       COMMANDS_VALIDATION → FIND_DUBLS → LOAD_PRIZNAKI

Каждый вызов процедуры выполняется в отдельном подключении (open → EXEC → commit → close),
чтобы избежать проблем MSSQL с несколькими транзакциями в одной сессии.

Best Practices:
  - airflow.sdk (Airflow 3.x Public Interface)
  - Тяжёлые импорты только внутри задач
  - Идемпотентность: партиция по data_interval, ORDER BY для детерминированного порядка
  - Логирование: прогресс, тайминги, ошибки — всё видно в логах задачи

Зависимость: apache-airflow-providers-microsoft-mssql
Connection:  mssql_default (Admin → Connections)
"""

import logging
import time

import pendulum
from airflow.sdk import DAG, task

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Конфигурация (при необходимости вынесите в Airflow Variable)
# ---------------------------------------------------------------------------
CONN_ID = "mssql_default"

GET_COMMANDS_SQL = """
SELECT KOM_NUMBER, COMMAND_DATE
FROM dbo.YOUR_TABLE
WHERE COMMAND_DATE >= CAST(%s AS DATE)
  AND COMMAND_DATE <  CAST(%s AS DATE)
ORDER BY KOM_NUMBER
"""

# Процедуры в порядке вызова. Формат: (полное_имя, человекочитаемый_шаг).
# Добавить/убрать шаг — достаточно изменить только этот список.
PROCEDURES = [
    ("dbo.COMMANDS_VALIDATION", "Валидация"),
    ("dbo.FIND_DUBLS", "Проверка дублей"),
    ("dbo.LOAD_PRIZNAKI", "Загрузка признаков"),
]

PROC_PARAM_NAME = "KOM_NUMBER"


# ---------------------------------------------------------------------------
# Вспомогательные функции (не задачи — вызываются внутри задач)
# ---------------------------------------------------------------------------
def _exec_procedure(conn_id: str, proc_name: str, param_name: str, param_value) -> float:
    """
    Выполняет одну хранимую процедуру в изолированном подключении.
    Возвращает время выполнения в секундах.

    Отдельное подключение на каждый вызов:
      open → EXEC → commit → close
    Это устраняет проблему MSSQL, когда две транзакции подряд
    в одной сессии приводят к ошибкам / зависаниям.
    """
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

    sql = f"EXEC {proc_name} @{param_name} = %s"
    hook = MsSqlHook(mssql_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    t0 = time.monotonic()
    try:
        cursor.execute(sql, (param_value,))
        conn.commit()
        elapsed = time.monotonic() - t0
        return elapsed
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# Задачи
# ---------------------------------------------------------------------------
@task
def get_commands():
    """
    Шаг 1: выборка KOM_NUMBER за интервал DAG Run.
    Логирует: параметры запроса, количество найденных строк, список KOM_NUMBER.
    """
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from airflow.sdk import get_current_context

    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    start_dt = dag_run.data_interval_start.strftime("%Y-%m-%d") if dag_run and dag_run.data_interval_start else None
    end_dt = dag_run.data_interval_end.strftime("%Y-%m-%d") if dag_run and dag_run.data_interval_end else None

    log.info("Выборка команд: interval=[%s, %s), conn_id=%s", start_dt, end_dt, CONN_ID)

    hook = MsSqlHook(mssql_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        t0 = time.monotonic()
        cursor.execute(GET_COMMANDS_SQL, (start_dt, end_dt))
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        elapsed = time.monotonic() - t0

        result = [dict(zip(columns, row)) for row in rows]
        # Сериализуем datetime → str для XCom
        for r in result:
            for k, v in list(r.items()):
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()

        kom_numbers = [r.get("KOM_NUMBER") for r in result]
        log.info(
            "Найдено %d команд за %.2f сек: %s",
            len(result),
            elapsed,
            kom_numbers,
        )
        if not result:
            log.warning("Пустой результат — процедуры вызываться не будут")

        return result
    finally:
        cursor.close()
        conn.close()


@task
def process_commands(commands: list[dict]):
    """
    Шаг 2: последовательная обработка каждой команды.
    Для каждого KOM_NUMBER по очереди вызывает все процедуры из PROCEDURES.
    Сначала полностью отрабатывается первая команда, потом вторая и т.д.

    Логирует:
      - Общий прогресс: «Команда 2/10, KOM_NUMBER=12345»
      - Каждый вызов процедуры: имя, KOM_NUMBER, время выполнения
      - Итоги: сколько команд обработано, общее время
      - При ошибке: какая процедура, какой KOM_NUMBER, номер команды
    """
    total = len(commands)
    if total == 0:
        log.info("Нет команд для обработки — задача завершена")
        return {"processed": 0, "total_seconds": 0.0}

    log.info("Начинаем обработку %d команд, процедур на каждую: %d", total, len(PROCEDURES))

    t_total = time.monotonic()
    processed = 0

    for idx, command in enumerate(commands, start=1):
        kom_number = command.get("KOM_NUMBER")
        if kom_number is None:
            log.error(
                "Команда %d/%d: поле KOM_NUMBER отсутствует. Запись: %s",
                idx, total, command,
            )
            raise ValueError(f"Команда {idx}/{total}: KOM_NUMBER отсутствует в записи {command}")

        log.info(
            "═══ Команда %d/%d  KOM_NUMBER=%s  COMMAND_DATE=%s ═══",
            idx, total, kom_number, command.get("COMMAND_DATE", "—"),
        )

        for step_num, (proc_name, step_label) in enumerate(PROCEDURES, start=1):
            log.info(
                "  [%d/%d] %s → %s (KOM_NUMBER=%s) ...",
                step_num, len(PROCEDURES), step_label, proc_name, kom_number,
            )
            try:
                elapsed = _exec_procedure(CONN_ID, proc_name, PROC_PARAM_NAME, kom_number)
                log.info(
                    "  [%d/%d] %s — OK за %.2f сек",
                    step_num, len(PROCEDURES), step_label, elapsed,
                )
            except Exception as e:
                log.error(
                    "  [%d/%d] %s — ОШИБКА: %s (KOM_NUMBER=%s, команда %d/%d)",
                    step_num, len(PROCEDURES), step_label, e, kom_number, idx, total,
                )
                raise RuntimeError(
                    f"Команда {idx}/{total}, KOM_NUMBER={kom_number}, "
                    f"шаг {step_num}/{len(PROCEDURES)} ({proc_name}): {e}"
                ) from e

        processed += 1
        log.info("  Команда %d/%d KOM_NUMBER=%s — все шаги завершены", idx, total, kom_number)

    total_seconds = time.monotonic() - t_total
    log.info(
        "Обработка завершена: %d/%d команд, общее время %.2f сек",
        processed, total, total_seconds,
    )
    return {"processed": processed, "total_seconds": round(total_seconds, 2)}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
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
    doc_md=__doc__,
) as dag:
    commands = get_commands()
    process_commands(commands)
