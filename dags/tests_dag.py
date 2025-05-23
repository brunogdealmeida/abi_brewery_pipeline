from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.jobs.tests.pipeline_tests import PipelineTest

def run_pipeline_tests():
    """Run tests for pipeline operations"""
    # Initialize test runner
    test_runner = PipelineTest()
    
    # Run all tests
    test_runner.run_all_tests()
    
    # Save results
    results_path = "include/tests_results/test_results.json"
    test_runner.save_results(results_path)
    
    return {"status": "success", "results_path": results_path}

with DAG(
    dag_id="tests_dag",
    schedule="@daily",
    start_date=datetime(2025, 5, 23),
    catchup=False,
    tags=["ab_inbev", "tests"]
) as dag:

    run_tests_task = PythonOperator(
        task_id="run_pipeline_tests",
        python_callable=run_pipeline_tests
    )
