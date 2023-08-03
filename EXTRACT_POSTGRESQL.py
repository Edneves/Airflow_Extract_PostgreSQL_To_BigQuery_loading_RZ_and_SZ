from airflow import DAG
from datetime import datetime
from global_modules.operators import Extract_PostgreSQL_To_BigQuery
from global_modules.functions import getLocalConfig
from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator,
                                                               BigQueryExecuteQueryOperator,
                                                               BigQueryCreateEmptyTableOperator)
from airflow.operators.empty import EmptyOperator
import json

DAG_ID = 'EXTRACT_POSTGRESQL'

yaml_data = getLocalConfig(DAG_ID)
params_connection = yaml_data['params']['connection']
info_tables = yaml_data['params']['info_tables']


# INFORMATION PROJECT GCP
project_id = info_tables['project_id']
location = info_tables['location']
credential = info_tables['credencial']
dataset_rz = info_tables['dataset_rz']
dataset_sz = info_tables['dataset_sz']
table_id = info_tables['table_id']
truncate_table_rz = f"TRUNCATE TABLE {project_id}.{dataset_rz}.{table_id}"
truncate_table_sz = f"TRUNCATE TABLE {project_id}.{dataset_sz}.{table_id}"

table_name = f"{dataset_rz}.{table_id}"

params_query = yaml_data['bq_comands'][f"insert_sez_{table_id}"]['params_query']

# INFORMATION CONNECTION AND EXTRACTION DATA POSTGRESQL
database = params_connection['database']
user = params_connection['user']
password = params_connection['password']
host = params_connection['host']
port = params_connection['port']
query_table = params_connection['query']

# INFORMATION TO DATAFRAME
listColumns = params_connection['list_columns']
granularity = params_connection['granularity']
nr_rows = params_connection['nr_rows']
column_movto = params_connection['column_movto']

TEMPLATE_SEARCH_PATH = f"{yaml_data['params']['template_search']['search_path']}/{DAG_ID}"
TEMPLATE_SEARCH_PATH_GLOBAL = yaml_data['params']['template_search']['search_path_global']

with open(f'{TEMPLATE_SEARCH_PATH}/schema_sz/{table_id}.json') as f_sz:
    schema_sz = json.load(f_sz)
    
with open(f'{TEMPLATE_SEARCH_PATH}/schema_rz/{table_id}.json') as f_rz:
    schema_rz = json.load(f_rz)

default_args = {
    'owner': 'Ednaldo',
    'start_date': datetime(2023, 7, 31),
    'depends_on_past': False
}


with DAG(
    DAG_ID,
    description='Read data PostgreSQL',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    template_searchpath=[TEMPLATE_SEARCH_PATH, TEMPLATE_SEARCH_PATH_GLOBAL]
) as dag:
    
    begin = EmptyOperator(
        task_id=f'begin_{table_id}'
    )

    create_structure_rz = BigQueryCreateEmptyTableOperator(
        task_id=f'create_structure_rz_{table_id}',
        project_id=project_id,
        dataset_id=dataset_rz,
        table_id=table_id,
        schema_fields=schema_rz,
        gcp_conn_id='google_cloud_default',
        exists_ok=True
    )
    
    truncate_rz_table = BigQueryInsertJobOperator(
        task_id=f"truncate_rz_{table_id}",
        configuration={
            "query": {
                "query": truncate_table_rz,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        location=location,
        gcp_conn_id='google_cloud_default',
    )
    
    extract = Extract_PostgreSQL_To_BigQuery(
        task_id=f'extract_data_{table_id}',
        db=database,
        user=user,
        password=password,
        host=host,
        port=port,
        listColumns=listColumns,
        queryTable=query_table,
        granularity=granularity,
        nr_rows=nr_rows,
        column_movto=column_movto,
        credential=credential,
        table_id=table_name,
        project_id=project_id
        
    )

    create_structure_sz = BigQueryCreateEmptyTableOperator(
        task_id=f'create_structure_sz_{table_id}',
        project_id=project_id,
        dataset_id=dataset_sz,
        table_id=table_id,
        schema_fields=schema_sz,
        gcp_conn_id='google_cloud_default',
        exists_ok=True
    )

    truncate_sz_table = BigQueryInsertJobOperator(
        task_id=f"truncate_sz_{table_id}",
        configuration={
            "query": {
                "query": truncate_table_sz,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        location=location,
        gcp_conn_id='google_cloud_default'
    )

    execute_sql = BigQueryInsertJobOperator(
        task_id=f"insert_sz_{table_id}",
        params={'target': f"{project_id}.{dataset_sz}.{table_id}",
                'source': f"{project_id}.{dataset_rz}.{table_id}"},
        configuration={
            "query": {
                "query": params_query,
                "useLegacySql": False,
            }
        },
        project_id=project_id,
        location=location,
        gcp_conn_id='google_cloud_default'
    )

    end = EmptyOperator(
        task_id=f'end_{table_id}'
    )

    begin >> create_structure_rz >> truncate_rz_table >> [extract, create_structure_sz] >> truncate_sz_table >> execute_sql >> end