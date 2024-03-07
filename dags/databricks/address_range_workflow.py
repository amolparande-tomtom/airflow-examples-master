from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
dag = DAG(
    'address_range_workflow',
    default_args=default_args,
    description='Address Range Workflow',
    schedule=None,
    params={
        "country": ["FRA"],
        "week": '32',
        "OrbisVenturaRelease": '24090.000'
    }
)
def build_zone_wise_notebook_params(**context):
    params = context["params"]
    country_list = params["country"]
    zone_wise_params = []
    for country in country_list:
        zone_wise_params.append({"country": country,
                                 "week": params["week"],
                                 "OrbisVenturaRelease": params["OrbisVenturaRelease"]
                                 })
    return zone_wise_params


build_zone_wise_input = PythonOperator(
    task_id='build_zone_wise_input',
    python_callable=build_zone_wise_notebook_params,
    provide_context=True,
    dag=dag
)

# Define the notebook task Genesis
genesis_address_ranges_task = DatabricksRunNowOperator.partial(
    task_id='genesis_address_ranges_task',
    databricks_conn_id='databricks_default',  # Connection ID to Databricks
    job_id="806933263803455",  # Job ID of the notebook
    dag=dag,
).expand(notebook_params=build_zone_wise_input.output)

# Define the notebook task Orbis
orbis_address_ranges_task = DatabricksRunNowOperator.partial(
    task_id='orbis_address_ranges_task',
    databricks_conn_id='databricks_default',  # Connection ID to Databricks
    job_id="827871496486372",  # Job ID of the notebook
    dag=dag,
).expand(notebook_params=build_zone_wise_input.output)

# Set task dependencies
build_zone_wise_input >> genesis_address_ranges_task >> orbis_address_ranges_task

