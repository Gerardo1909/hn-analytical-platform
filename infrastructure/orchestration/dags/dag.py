"""
DAG de orquestación del pipeline ETL de HackerNews.
"""

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="hn_etl",
    description="ETL pipeline de HackerNews",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["hackernews", "etl"],
)
def hn_etl():
    """
    Pipeline ETL:
    ingestion -> processing -> transformation
    """

    # Argumentos comunes para todos los contenedores efímeros
    common_docker_args = {
        "image": "hn-analytical-platform:prod",
        "api_version": "auto",
        "auto_remove": "force",
        "network_mode": "docker_airflow-network",
        "docker_url": "unix://var/run/docker.sock",
        "mount_tmp_dir": False,  # Evita problemas de permisos con el directorio temporal de Airflow
        "environment": {
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL"),
            "AWS_BUCKET_NAME": os.getenv("AWS_BUCKET_NAME"),
        },
    }

    ingestion = DockerOperator(
        task_id="ingestion",
        command="python -m ingestion.main",
        **common_docker_args,
    )

    processing = DockerOperator(
        task_id="processing",
        command="python -m processing.main {{ data_interval_end.strftime('%Y-%m-%d') }}",
        **common_docker_args,
    )

    transformation = DockerOperator(
        task_id="transformation",
        command="python -m transformation.main {{ data_interval_end.strftime('%Y-%m-%d') }}",
        **common_docker_args,
    )

    ingestion >> processing >> transformation


# Instancia del DAG
hn_etl()
