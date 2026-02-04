"""
DAG с DockerOperator: запуск контейнера как задачи.

Выполняет команду внутри Docker-образа (образ можно указать свой).
Зависимость: apache-airflow-providers-docker
Требуется: Docker Engine доступен с хоста, где запущен worker (или DOCKER_HOST).
"""

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

with DAG(
    dag_id="docker_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["docker", "containers"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1), "owner": "airflow"},
) as dag:
    docker_task = DockerOperator(
        task_id="docker_run",
        image="alpine:3.19",
        api_version="auto",
        auto_remove=True,
        command="echo 'Hello from Docker' && date",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )
