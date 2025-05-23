
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.jobs import landing, bronze, silver, gold
from include.jobs.data_quality import run_quality_checks

with DAG(
    dag_id="brewery_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ab_inbev"]
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_api_to_landing",
        python_callable=landing.fetch_api_to_landing
    )

    validate_data = PythonOperator(
        task_id="validate_to_bronze_or_fail",
        python_callable=bronze.validate_landing_to_bronze_or_fail
    )

    transform_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=silver.transform_to_silver
    )

    transform_gold = PythonOperator(
        task_id="transform_to_gold",
        python_callable=gold.transform_to_gold
    )

    quality_check = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_quality_checks
    )
    
    fetch_data >> validate_data >> transform_silver >> transform_gold >> quality_check
