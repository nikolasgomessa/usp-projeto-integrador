from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import sys
import os
import datetime
from abc import ABC, abstractmethod
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import re
from urllib.parse import urlencode, quote, urlparse


# Create a spark session
spark = SparkSession.builder \
    .appName(f'transform_delivery') \
    .enableHiveSupport() \
    .getOrCreate()


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

        transformed_df = self.df.withColumn("answers", F.col("queries").getItem(0)['answers'])

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
    
        return transformed_df

  
extract_data = ExtractData(
    spark=spark,
    file_directory='s3://775307465848-trusted/', 
    file_type='parquet'
)

df_raw = extract_data.extract()

print("Fim extract")

transform_data = Transformation(df_raw, spark)
df_ooni = transform_data.transform()

print("Fim transform")

print("Comeco write")

output_directory = 's3://775307465848-delivery/delivery'
write_data = DataWriter()
write_data.write_parquet(df_ooni, output_directory, ['bucket_date', 'probe_cc'])

print("Fim write")