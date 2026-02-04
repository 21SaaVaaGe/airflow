"""
DAG с Apache Spark: SparkSubmitOperator.

Запуск Spark-приложения (JAR или PySpark) через spark-submit.
Зависимость: apache-airflow-providers-apache-spark
Connection: spark_default (Host = master URL, например spark://spark-master:7077 или k8s://...).
"""

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

with DAG(
    dag_id="spark_operator_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "batch"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2), "owner": "airflow"},
) as dag:
    spark_submit = SparkSubmitOperator(
        task_id="spark_submit_job",
        application="/opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
        java_class="org.apache.spark.examples.SparkPi",
        application_args=["10"],
        conn_id="spark_default",
        verbose=True,
    )
