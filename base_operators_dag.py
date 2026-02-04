"""
DAG с базовыми операторами Airflow (широко распространённые).

BashOperator, EmptyOperator, PythonOperator — цепочка с ветвлением.
Зависимость: встроено в Airflow (провайдер standard).
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def python_hello(**context):
    """Callable для PythonOperator."""
    from airflow.sdk import get_current_context

    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    interval = dag_run.data_interval_start if dag_run else None
    print(f"Hello from PythonOperator. Interval: {interval}")
    return "done"


with DAG(
    dag_id="base_operators_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["base", "example", "bash", "python", "empty"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1), "owner": "airflow"},
) as dag:
    start = EmptyOperator(task_id="start")

    bash_echo = BashOperator(
        task_id="bash_echo",
        bash_command="echo 'BashOperator: Hello' && date",
    )

    python_task = PythonOperator(
        task_id="python_hello",
        python_callable=python_hello,
    )

    bash_list = BashOperator(
        task_id="bash_list",
        bash_command="ls -la || true",
    )

    end = EmptyOperator(task_id="end")

    start >> bash_echo >> python_task >> bash_list >> end
