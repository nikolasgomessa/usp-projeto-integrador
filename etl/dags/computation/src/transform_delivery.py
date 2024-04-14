
import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from abc import ABC, abstractmethod
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import re
from urllib.parse import urlencode, quote, urlparse


args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'bucket_trusted',
                           'bucket_delivery'])

TITLE_REGEXP = re.compile("<title.*?>(.*?)</title>", re.IGNORECASE | re.DOTALL)
META_TITLE_REGEXP = re.compile("<meta.*?property=\"og:title\".*?content=\"(.*?)\"", re.IGNORECASE | re.DOTALL)

# Define as funcoes UDFs
def get_last_response_body(msmt):
    try:
        # Obt�m o corpo da �ltima resposta
        body = msmt[0]['response']['body']
        return body
    except (KeyError, TypeError, IndexError):
        return ''

def get_http_title(msmt):
    body = get_last_response_body(msmt)
    # Se o corpo n�o for uma string, retornamos uma string vazia
    if not isinstance(body, str):
        return ''
    # Procura o t�tulo no corpo da resposta usando a express�o regular TITLE_REGEXP
    m = TITLE_REGEXP.search(body)
    if m:
        return m.group(1)
    return ''

def get_meta_http_title(msmt):
    body = get_last_response_body(msmt)
    if not isinstance(body, str):
        return ''
    # Procura o t�tulo no corpo da resposta usando a express�o regular META_TITLE_REGEXP
    m = META_TITLE_REGEXP.search(body)
    if m:
        return m.group(1)
    return ''

def get_domain_urlparse(msmt):
    return urlparse(msmt).netloc


class DataWriter:
    def write_parquet(self, df, output_directory, partitioned_by, mode="overwrite"):
        df.write.partitionBy(partitioned_by).mode(mode).parquet(output_directory)


class UnsupportedFileType(Exception):
    def __init__(self, file_type):
        self.file_type = file_type
        self.message = f"File(s) of type {file_type} not supported"
        super().__init__(self.message)

class ExtractData:
    def __init__(self, spark, file_directory: list, file_type: str):
        self.file_directory = file_directory
        self.file_type = file_type
        self.spark = spark

    def extract(self) -> DataFrame:
        if self.file_type in ('parquet'):
            return self.spark.read.parquet(self.file_directory)
        else:
            raise UnsupportedFileType(self.file_type)
        
class TransformData(ABC):
    """
    Gathers general functions for all transformations.
    """
    @abstractmethod
    def transform(self) -> DataFrame:
        pass


class Transformation(TransformData):
    """
    Functions for transforming the pandas dataframe for banks.
    """
    def __init__(self, df: DataFrame, spark):
        """
        Receives the dataframe.
        """
        self.df = df
        self.spark = spark
        
    def transform(self) -> DataFrame:

        transformed_df = self.df.withColumn("answers", F.col("queries")['answers'])

        transformed_df = transformed_df.withColumn("dns_resolved_ips_full", F.expr("transform(answers, x -> x.ipv4)"))
        transformed_df = transformed_df.withColumn("dns_resolved_ips", F.expr("filter(dns_resolved_ips_full, x -> x is not null)"))
        # Registra as UDFs
        get_last_response_body_udf = udf(get_last_response_body, StringType())
        get_http_title_udf = udf(get_http_title, StringType())
        get_meta_http_title_udf = udf(get_meta_http_title, StringType())
        get_domain_urlparse_udf = udf(get_domain_urlparse, StringType())
        transformed_df = transformed_df.withColumn("http_title", get_http_title_udf("requests"))
        transformed_df = transformed_df.withColumn("http_meta_title", get_meta_http_title_udf("requests"))
        transformed_df = transformed_df.withColumn("domain", get_domain_urlparse_udf("input"))
        
        columns_drop = ["queries", "answers", "requests", "control"]
        transformed_df = transformed_df.drop(*columns_drop)

        return transformed_df
extract_data = ExtractData(
    spark=spark,
    file_directory=f"s3://{args['bucket_trusted']}/",
    file_type='parquet'
)
df_raw = extract_data.extract()

transform_data = Transformation(df_raw, spark)
df_ooni = transform_data.transform()

output_directory = f"s3://{args['bucket_delivery']}/delivery"
write_data = DataWriter()
write_data.write_parquet(df_ooni, output_directory, ['bucket_date'])

query_create_table = """
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
    probe_cc STRING)
PARTITIONED BY(bucket_date STRING) 
STORED AS PARQUET LOCATION 's3://771030032684-delivery/delivery/';
"""

spark.sql("create schema if not exists ooni_data")
spark.sql(query_create_table)
spark.sql("MSCK REPAIR TABLE ooni_data.tb_delivery")

job.commit()