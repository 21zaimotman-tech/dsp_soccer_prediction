from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations.expectation import ExpectationConfiguration

default_args = {'owner': 'airflow', 'start_date': datetime(2026, 1, 1)}

with DAG('soccer_ingestion', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    def ingest_and_validate():
        # Requirement: Internal container paths must match mapped volumes
        raw_path, good_path, bad_path = '/opt/airflow/data/raw_data', '/opt/airflow/data/good_data', '/opt/airflow/data/bad_data'
        
        files = [f for f in os.listdir(raw_path) if f.endswith('.csv')]
        if not files: return
        
        file_name = files[0]
        df = pd.read_csv(os.path.join(raw_path, file_name))
        
        # Modern GX v1.0+ initialization
        context = gx.get_context(context_root_dir='/opt/airflow/gx')
        suite_name = "soccer_quality_suite"
        
        try:
            suite = context.suites.get(name=suite_name)
        except:
            suite = context.suites.add(ExpectationSuite(name=suite_name))
        
        # Requirement: Ingestion job must validate data quality
        suite.add_expectation(ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between", 
            kwargs={"min_value": 1}
        ))
        
        validator = context.get_validator(
            batch_request={"runtime_parameters": {"batch_data": df}, "batch_identifiers": {"default_identifier_name": "soccer_batch"}},
            expectation_suite_name=suite_name
        )
        results = validator.validate()
        
        # Requirement: Save ingestion stats to PostgreSQL for monitoring
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        status = "Good" if results.success else "Bad"
        pg_hook.run(f"INSERT INTO ingestion_stats (file_name, status) VALUES ('{file_name}', '{status}')")
        
        # Requirement: Move files based on validation result
        dest = good_path if results.success else bad_path
        os.rename(os.path.join(raw_path, file_name), os.path.join(dest, file_name))

    validate_task = PythonOperator(task_id='validate_soccer_batch', python_callable=ingest_and_validate)