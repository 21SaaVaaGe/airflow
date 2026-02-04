# DAGs и зависимости

| DAG | Файл | Провайдер / зависимости | Connection ID |
|-----|------|------------------------|---------------|
| PostgreSQL | `postgres_dag.py` | apache-airflow-providers-postgres | postgres_default |
| Spark | `spark_operator_dag.py` | apache-airflow-providers-apache-spark | spark_default |
| Docker | `docker_dag.py` | apache-airflow-providers-docker | — (Docker daemon) |
| S3 / SeaweedFS | `s3_seaweedfs_dag.py` | apache-airflow-providers-amazon | s3_default (для SeaweedFS: endpoint_url в Extra) |
| ClickHouse | `clickhouse_dag.py` | apache-airflow-providers-clickhouse | clickhouse_default |
| Базовые операторы | `base_operators_dag.py` | apache-airflow-providers-standard | — |

Установка провайдеров:
```bash
pip install apache-airflow-providers-postgres apache-airflow-providers-apache-spark
pip install apache-airflow-providers-docker apache-airflow-providers-amazon
pip install apache-airflow-providers-clickhouse apache-airflow-providers-standard
```
