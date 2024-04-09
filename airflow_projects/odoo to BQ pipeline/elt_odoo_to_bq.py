from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import odoorpc
import pandas as pd
from google.cloud import bigquery

def extract_data_from_odoo(ds, **kwargs):
    # Connect to Odoo database using odoorpc
    odoo = odoorpc.ODOO('https://worky-sapi.odoo.com', port=8069)
    odoo.login('odoo-ps-worky-sapi-production-3344281', 'mteran@worky.mx', 'PraseodimioOdoo')
    
    # Get a list of all tables in the database
    model_names = odoo.env['ir.model'].search([])
    model_objs = [odoo.env[model.model] for model in model_names]
    
    # Extract data from tables that have a record count greater than 0
    data = {}
    for model in model_objs:
        record_count = model.search_count([])
        if record_count > 0:
            model_data = model.read()
            data[model.name] = pd.DataFrame(model_data)
    
    return data

def load_data_to_bigquery(ds, **kwargs):
    # Load extracted data into BigQuery
    bigquery_client = bigquery.Client()
    for table_name, table_data in kwargs['task_instance'].xcom_pull(task_ids='extract_data_from_odoo').items():
        table_id = f"{ds}.{table_name}"
        table = bigquery_client.create_table(bigquery.Table(table_id))
        bigquery_client.load_table_from_dataframe(table_data, table_id).result()

dag = DAG(
    'odoo_to_bigquery',
    description='Extract data from Odoo and load into BigQuery',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 2, 9),
    catchup=False
)

extract_data_task = PythonOperator(
    task_id='extract_data_from_odoo',
    python_callable=extract_data_from_odoo,
    provide_context=True,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data_to_bigquery',
    python_callable=load_data_to_bigquery,
    provide_context=True,
    dag=dag,
    trigger_rule='all_done'
)

extract_data_task >> load_data_task
