import os
import json
import itertools
import logging
from datetime import datetime, timedelta
import boto3
###
from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator


def get_aws_credentials(conn_id):
    session = Connection.get_connection_from_secrets(conn_id)
    return {
        'aws_access_key_id': session.extra_dejson.get('aws_access_key_id'),
        'aws_secret_access_key': session.extra_dejson.get('aws_secret_access_key'),
        'aws_session_token': session.extra_dejson.get('aws_session_token'),
        'region_name': session.extra_dejson.get('region_name')
    }

def run_raw(**kwargs):
    source_bucket = 'ooni-data'
    dest_bucket = '975050372651-raw'
    source_prefix = f'autoclaved/jsonl/{kwargs["params"]["ano"]}'
    aws_conn_id = "aws_default"
    desired_test_type = "web_connectivity"

    countries_to_consider = ["BR", "CN", "FR", "RU", "GB", "US", "DE", "IN", "AR"]


    credentials = get_aws_credentials(aws_conn_id)

    s3_client = boto3.client(
        's3',
        **credentials
    )

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)
    
    for page in pages:
        for obj in page['Contents']:
            source_key = obj['Key']
            source_date = source_key.split('/')[2]
            split_data_type = source_key.split('/')[3].split('-')

            country = split_data_type[1]
            test_type = split_data_type[3]

            if test_type == desired_test_type and country in countries_to_consider:
                dest_key = os.path.join(source_date, country, test_type, os.path.basename(source_key))
                
                # Verifica se o arquivo já existe no bucket de destino
                try:
                    s3_client.head_object(Bucket=dest_bucket, Key=dest_key)
                    # Se o arquivo já existe, não faz nada
                    print(f"O arquivo {dest_key} já existe no bucket de destino.")
                except:
                    # Se o arquivo não existe, faça a cópia do objeto
                    print(f"Copiando {source_key} para {dest_key}")
                    response = s3_client.copy_object(
                        Bucket=dest_bucket,
                        CopySource={'Bucket': source_bucket, 'Key': source_key},
                        Key=dest_key
                    )
# ---------- Pipeline Tasks ----------------------------------------------------------------------------------------- #

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": datetime(2023, 8, 1)
}

with DAG('get_raw_data',
         schedule_interval= None,  
         catchup=False, 
         max_active_runs=45, 
         default_args=default_args,
         tags=['s3']) as dag:


    execute_python_raw = PythonOperator(
        dag=dag,
        task_id="execute_python_raw",
        python_callable=run_raw
    )

    execute_python_raw