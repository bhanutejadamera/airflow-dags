from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Log who triggered the DAG (prints to task log)
def log_user_info():
    import os
    user = os.getenv("USER", "Unknown")
    print(f"DAG triggered by user: {user}")

# Default args
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    dag_id='refresh_unified_view',
    default_args=default_args,
    schedule_interval='@daily',  # or use None for manual only
    catchup=False,
    tags=['bigquery', 'view', 'etl'],
    description='Refreshes the unified view by combining 3 BigQuery tables',
) as dag:

    log_user = PythonOperator(
        task_id='log_trigger_user',
        python_callable=log_user_info
    )

    refresh_view = BigQueryInsertJobOperator(
        task_id='refresh_bigquery_unified_view',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE VIEW `stackoverflow-data-pipeline.stackoverflow_dataset.unified_view` AS
                    SELECT 
                      `Customer Id` AS id,
                      `First Name` AS first_name,
                      `Last Name` AS last_name,
                      Email AS email,
                      Country AS country,
                      'customers' AS source
                    FROM `stackoverflow-data-pipeline.stackoverflow_dataset.customers`

                    UNION ALL

                    SELECT 
                      `Organization Id` AS id,
                      Name AS first_name,
                      NULL AS last_name,
                      NULL AS email,
                      Country AS country,
                      'organizations' AS source
                    FROM `stackoverflow-data-pipeline.stackoverflow_dataset.organizations`

                    UNION ALL

                    SELECT 
                      `User Id` AS id,
                      `First Name` AS first_name,
                      `Last Name` AS last_name,
                      Email AS email,
                      NULL AS country,
                      'people' AS source
                    FROM `stackoverflow-data-pipeline.stackoverflow_dataset.people`
                """,
                "useLegacySql": False,
            }
        },
        location="US",  # Same region as your dataset
    )

    # Define task order
    log_user >> refresh_view
