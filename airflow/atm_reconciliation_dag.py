from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='atm_reconciliation_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_reconciliation = BashOperator(
        task_id='run_spark_reconciliation',
        bash_command='echo "Running reconciliation..."'
    )

    run_reconciliation