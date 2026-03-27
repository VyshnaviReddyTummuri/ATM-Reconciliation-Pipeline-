from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="atm_reconciliation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Pipeline Started'"
    )

    run_reconciliation = BashOperator(
        task_id="run_spark_reconciliation",
        bash_command="""
docker exec atm-recon-spark-master /opt/spark/bin/spark-submit \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
--conf spark.jars.ivy=/tmp/.ivy2 \
/spark/atm_reconciliation_job.py
"""
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Pipeline Completed'"
    )

    start >> run_reconciliation >> end