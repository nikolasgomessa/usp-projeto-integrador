import itertools
import json
import logging
import os
from datetime import datetime, timedelta

import boto3
###
from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator

def get_aws_credentials(conn_id):
    session = Connection.get_connection_from_secrets(conn_id)
    return {
        "aws_access_key_id": session.extra_dejson.get("aws_access_key_id"),
        "aws_secret_access_key": session.extra_dejson.get("aws_secret_access_key"),
        "aws_session_token": session.extra_dejson.get("aws_session_token"),
        "region_name": session.extra_dejson.get("region_name"),
    }


def run_raw(date, **kwargs):
    source_bucket = "ooni-data"
    dest_bucket = "771030032684-raw"
    source_prefix = f'autoclaved/jsonl/{date}'
    aws_conn_id = "aws_default"
    desired_test_type = "web_connectivity"

    countries_to_consider = ["BR", "DE", "RU", "US"]

    credentials = get_aws_credentials(aws_conn_id)

    s3_client = boto3.client("s3", **credentials)

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)

    limit_per_country = 1.5
    min_file_size = 0.3
    count_size_country = {}

    for page in pages:
        for obj in page["Contents"]:
            source_key = obj["Key"]
            source_date = source_key.split("/")[2]
            split_data_type = source_key.split("/")[3].split("-")

            country = split_data_type[1]
            test_type = split_data_type[3]

            if source_date not in count_size_country:
                count_size_country[source_date] = {}

            if country not in count_size_country[source_date]:
                count_size_country[source_date][country] = 0

            if count_size_country[source_date][country] <= limit_per_country:
                get_object = s3_client.head_object(Bucket=source_bucket, Key=source_key)
                size_file_bytes = get_object["ContentLength"]
                size_file_mb = size_file_bytes / (1024 * 1024)

                if (
                    test_type == desired_test_type
                    and country in countries_to_consider
                    and size_file_mb >= min_file_size
                    and (count_size_country[source_date][country] + size_file_mb)
                    <= limit_per_country
                ):
                    dest_key = os.path.join(source_date, os.path.basename(source_key))

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
                            CopySource={"Bucket": source_bucket, "Key": source_key},
                            Key=dest_key,
                        )
                        count_size_country[source_date][country] = (
                            count_size_country[source_date][country] + size_file_mb
                        )

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": datetime(2023, 8, 1),
}

with DAG(
    "ooni-etl",
    schedule_interval=None,
    description='OONI data transformation',
    catchup=False,
    max_active_runs=5,
    default_args=default_args,
    tags=["etl"],
) as dag:

    etl_start = DummyOperator(
        task_id='etl_start'
    )
    
    ingestion_tasks = []
    for month in range(1, 11):
        month_lpad = str(month).rjust(2, "0")        
        ingestion_tasks.append(
            PythonOperator(
                dag=dag,
                task_id=f'ingestion_raw_{month_lpad}',
                python_callable=run_raw,
                op_kwargs={"date": f'2020-{month_lpad}'}
            )
        )

    trusted_task = GlueJobOperator(
        task_id='etl_raw_to_trusted',
        job_name='transform_trusted',
        region_name='us-east-1',
        iam_role_name='LabRole',
        script_location='s3://771030032684-scripts/src/transform_trusted.py',
        create_job_kwargs={"GlueVersion": "4.0", "NumberOfWorkers": 10, "WorkerType": "G.1X", 
                           "DefaultArguments": {
                                '--extra-py-files': 's3://771030032684-dependencies/dependencies/pydeequ.zip',
                                '--extra-jars': 's3://771030032684-dependencies/dependencies/deequ-2.0.4-spark-3.3.jar'
                            }, 
                        },
        script_args = {
            "--bucket_trusted": "574356460190-trusted",
            "--bucket_logs": "574356460190-logs",
            "--bucket_raw": "771030032684-raw"
        },
        s3_bucket = "aws-glue-assets-574356460190-us-east-1"
    )

    delivery_task = GlueJobOperator(
        task_id='etl_trusted_to_delivery',
        job_name='transform_delivery',
        region_name='us-east-1',
        iam_role_name='LabRole',
        script_location='s3://771030032684-scripts/src/transform_delivery.py',
        create_job_kwargs={"GlueVersion": "4.0", "NumberOfWorkers": 10, "WorkerType": "G.1X"},
        script_args = {
            "--bucket_trusted": "574356460190-trusted",
            "--bucket_delivery": "574356460190-delivery"
        },
        s3_bucket = "aws-glue-assets-574356460190-us-east-1"
    )

    LOG_QUERY_CREATE_TABLE = """
        CREATE EXTERNAL TABLE IF NOT EXISTS ooni_data.tb_processamento_job ( check string,
            check_level string,
            check_status string,
            constraint string,
            constraint_status string,
            constraint_message string,
            bucket_date string ) 
        STORED AS PARQUET LOCATION 's3://574356460190-logs/validation/trusted/'
    """

    DELIVERY_QUERY_CREATE_TABLE = """
        CREATE EXTERNAL TABLE IF NOT EXISTS ooni_data.tb_delivery ( 
            id STRING, 
            measurement_start_time TIMESTAMP, 
            test_start_time TIMESTAMP, 
            probe_asn STRING, 
            probe_ip STRING, 
            report_id STRING, 
            test_name STRING, 
            control_failure STRING, 
            blocking STRING, 
            http_experiment_failure STRING, 
            dns_experiment_failure STRING, 
            platform STRING, 
            domain STRING, 
            http_title STRING, 
            http_meta_title STRING, 
            probe_cc STRING,
            bucket_date DATE)
        STORED AS PARQUET LOCATION 's3://771030032684-delivery/delivery/'
    """

    athena_logs_table_task = AWSAthenaOperator(
        task_id='log_create_table',
        query=LOG_QUERY_CREATE_TABLE,
        database="ooni_data",
        output_location=f's3://574356460190-athena',
        sleep_time=30,
        max_tries=None,
        aws_conn_id="aws_default"
    )


    end_dag = DummyOperator(task_id="end_dag")

    etl_start >> ingestion_tasks >> trusted_task >> athena_logs_table_task >> delivery_task >> end_dag