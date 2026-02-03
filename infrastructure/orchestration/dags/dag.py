"""
DAG de orquestación del pipeline ETL de HackerNews.
"""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="hn_etl",
    description="ETL pipeline de HackerNews",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["hackernews", "etl"],
)
def hn_etl():
    """
    Pipeline ETL:
    ingestion -> processing -> transformation
    """

    @task
    def ingestion():
        from src.ingestion.main import run

        run()

    @task
    def processing():
        from src.processing.main import run

        run()

    @task
    def transformation():
        from src.transformation.main import run

        run()

    ingestion() >> processing() >> transformation()


# Instancia del DAG
hn_etl()
