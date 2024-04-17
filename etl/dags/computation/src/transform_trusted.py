
import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from abc import ABC, abstractmethod
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import datetime
import boto3
import gc
import threading

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
os.environ["SPARK_VERSION"]='3.3'

from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.analyzers import *

class DataWriter:
    def write_parquet(self, df, output_directory, partitioned_by, mode="overwrite"):
        df.write.format("parquet").partitionBy(partitioned_by).mode(mode).save(output_directory)


args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'bucket_logs',
                           'bucket_trusted',
                           'bucket_raw'])

SCHEMA = StructType([
    StructField("test_keys", StructType([
        StructField("accessible", StringType(), True),
        StructField("agent", StringType(), True),
        StructField("blocking", StringType(), True),
        StructField("body_length_match", StringType(), True),
        StructField("body_proportion", DoubleType(), True),
        StructField("client_resolver", StringType(), True),
        StructField("control", StructType([
            StructField("dns", StructType([
                StructField("addrs", ArrayType(StringType(), True), True),
                StructField("failure", StringType(), True)
            ]), True),
            StructField("http_request", StructType([
                StructField("body_length", LongType(), True),
                StructField("failure", StringType(), True),
                StructField("headers", StructType([
                    StructField("Accept-Ranges", StringType(), True),
                    StructField("CF-RAY", StringType(), True),
                    StructField("Cache-Control", StringType(), True),
                    StructField("Content-Language", StringType(), True),
                    StructField("Content-Location", StringType(), True),
                    StructField("Content-Type", StringType(), True),
                    StructField("Date", StringType(), True),
                    StructField("ETag", StringType(), True),
                    StructField("Expires", StringType(), True),
                    StructField("Last-Modified", StringType(), True),
                    StructField("Server", StringType(), True),
                    StructField("Set-Cookie", StringType(), True),
                    StructField("Strict-Transport-Security", StringType(), True),
                    StructField("TCN", StringType(), True),
                    StructField("Vary", StringType(), True),
                    StructField("X-Content-Type-Options", StringType(), True),
                    StructField("X-Frame-Options", StringType(), True),
                    StructField("X-Xss-Protection", StringType(), True),
                    StructField("content-encoding", StringType(), True)
                ]), True),
                StructField("status_code", LongType(), True),
                StructField("title", StringType(), True)
            ]), True)
        ]), True),
        StructField("control_failure", StringType(), True),
        StructField("dns_consistency", StringType(), True),
        StructField("dns_experiment_failure", StringType(), True),
        StructField("headers_match", StringType(), True),
        StructField("http_experiment_failure", StringType(), True),
        StructField("queries", ArrayType(StructType([
            StructField("answers", ArrayType(StructType([
                StructField("answer_type", StringType(), True),
                StructField("ipv4", StringType(), True)
            ]), True), True),
            StructField("failure", StringType(), True),
            StructField("hostname", StringType(), True),
            StructField("query_type", StringType(), True),
            StructField("resolver_hostname", StringType(), True),
            StructField("resolver_port", StringType(), True)
        ]), True), True),
        StructField("requests", ArrayType(StructType([
            StructField("failure", StringType(), True),
            StructField("request", StructType([
                StructField("body", StringType(), True),
                StructField("headers", StructType([
                    StructField("Accept", StringType(), True),
                    StructField("Accept-Language", StringType(), True),
                    StructField("User-Agent", StringType(), True)
                ]), True),
                StructField("method", StringType(), True),
                StructField("tor", StructType([
                    StructField("exit_ip", StringType(), True),
                    StructField("exit_name", StringType(), True),
                    StructField("is_tor", StringType(), True)
                ]), True),
                StructField("url", StringType(), True)
            ]), True),
            StructField("response", StructType([
                StructField("body", StringType(), True),
                StructField("code", LongType(), True),
                StructField("headers", StructType([
                    StructField("Accept-Ranges", StringType(), True),
                    StructField("Cache-Control", StringType(), True),
                    StructField("Content-Language", StringType(), True),
                    StructField("Content-Location", StringType(), True),
                    StructField("Content-Type", StringType(), True),
                    StructField("Date", StringType(), True),
                    StructField("ETag", StringType(), True),
                    StructField("Expires", StringType(), True),
                    StructField("Last-Modified", StringType(), True),
                    StructField("Location", StringType(), True),
                    StructField("Server", StringType(), True),
                    StructField("Strict-Transport-Security", StringType(), True),
                    StructField("TCN", StringType(), True),
                    StructField("Vary", StringType(), True),
                    StructField("X-Content-Type-Options", StringType(), True),
                    StructField("X-Frame-Options", StringType(), True),
                    StructField("X-Xss-Protection", StringType(), True),
                    StructField("content-encoding", StringType(), True)
                ]), True)
            ]), True)
        ]), True), True),
        StructField("retries", LongType(), True),
        StructField("socksproxy", StringType(), True),
        StructField("status_code_match", StringType(), True),
        StructField("tcp_connect", ArrayType(StructType([
            StructField("ip", StringType(), True),
            StructField("port", LongType(), True),
            StructField("status", StructType([
                StructField("blocked", StringType(), True),
                StructField("failure", StringType(), True),
                StructField("success", StringType(), True)
            ]), True)
        ]), True), True),
        StructField("title_match", StringType(), True)
    ]), True),
    StructField("annotations", StructType([
        StructField("platform", StringType(), True)
    ]), True),
    StructField("backend_version", StringType(), True),
    StructField("bucket_date", StringType(), True),
    StructField("data_format_version", StringType(), True),
    StructField("id", StringType(), True),
    StructField("input", StringType(), True),
    StructField("input_hashes", ArrayType(StringType(), True), True),
    StructField("measurement_start_time", StringType(), True),
    StructField("options", ArrayType(StringType(), True), True),
    StructField("probe_asn", StringType(), True),
    StructField("probe_cc", StringType(), True),
    StructField("probe_city", StringType(), True),
    StructField("probe_ip", StringType(), True),
    StructField("report_filename", StringType(), True),
    StructField("report_id", StringType(), True),
    StructField("software_name", StringType(), True),
    StructField("software_version", StringType(), True),
    StructField("test_helpers", StructType([
        StructField("backend", StructType([
            StructField("address", StringType(), True),
            StructField("type", StringType(), True)
        ]), True)
    ]), True),
    StructField("test_name", StringType(), True),
    StructField("test_runtime", DoubleType(), True),
    StructField("test_start_time", StringType(), True),
    StructField("test_version", StringType(), True)
])

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
        if self.file_type in ('json'):
            return self.spark.read.format("json")\
                .option("recursiveFileLookup", "true")\
                .option("pathGlobFilter","*.json")\
            .load(self.file_directory, schema=SCHEMA)
        else:
            raise UnsupportedFileType(self.file_type)


class TransformData(ABC):
    """
    Gathers general functions for all transformations.
    """
    @abstractmethod
    def transform(self) -> DataFrame:
        pass
    
    @abstractmethod
    def data_quality(self):
        pass


class Transformation(TransformData):
    """
    Functions for transforming the pandas dataframe for banks.
    """
    def __init__(self, df: DataFrame, spark, bucket_date):
        """
        Receives the dataframe.
        """
        self.df = df
        self.spark = spark
        self.bucket_date = bucket_date


    def transform(self) -> DataFrame:

        transformed_df = self.df \
        .select(
            F.col('id'),
            F.col('input'),
            F.col('measurement_start_time'),
            F.col('test_start_time'),
            F.col('probe_asn'),
            F.col('probe_cc'),
            F.col('probe_ip'),
            F.col('report_id'),
            F.col('software_name'),
            F.col('software_version'),
            F.col('test_name'),
            F.col('test_runtime'),
            F.col('test_version'),
            F.col('bucket_date'),
            F.col('test_keys.queries').getItem(0).alias('queries'),
            F.col('test_keys.control_failure').alias('control_failure'),
            F.col('test_keys.blocking').alias('blocking'),
            F.col('test_keys.http_experiment_failure').alias('http_experiment_failure'),
            F.col('test_keys.tcp_connect').alias('tcp_connect'),
            F.col('test_keys.requests').alias('requests'),
            F.col('test_keys.control').alias('control'),
            F.col('test_keys.dns_experiment_failure').alias('dns_experiment_failure'),
            F.col('annotations.platform').alias('platform')
        )
        transformed_df = transformed_df.cache()
        
        transformed_df = transformed_df.withColumn("bucket_date", F.to_date(F.col("bucket_date")))\
                                       .withColumn("measurement_start_time", F.to_timestamp("measurement_start_time"))\
                                       .withColumn("test_start_time", F.to_timestamp("test_start_time"))
        transformed_df = transformed_df.cache()
        transformed_df = transformed_df.withColumn("row_id", F.md5(F.concat(F.col("id"), F.col("measurement_start_time"), F.col("input"))))\
                                       .dropDuplicates(["row_id"])
        
                                    #    .withColumn("is_ip_valid", F.when(F.col("probe_ip").rlike(r"^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$"), F.lit(True)))\
        transformed_df = transformed_df.cache()
            
        # transformed_df = transformed_df.withColumn("is_url_valid", F.when(F.col("input").rlike(r"^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$"), F.lit(True)))\
                                       

    
        return transformed_df

    def data_quality(self, df):

        check = Check(self.spark, CheckLevel.Warning, "Review Check")
        check_result = (VerificationSuite(self.spark)
                        .onData(df)
                        .addCheck(check
                        .isUnique("row_id")
                        .hasCompleteness("id", lambda completeness: completeness >= 0.9)
                        .hasCompleteness("probe_asn", lambda completeness: completeness >= 0.9)
                        .hasCompleteness("probe_cc", lambda completeness: completeness >= 0.98)
                        .hasCompleteness("probe_ip", lambda completeness: completeness >= 0.9)
                        .hasCompleteness("test_start_time", lambda completeness: completeness >= 0.9)
                        .hasCompleteness("test_name", lambda completeness: completeness >= 0.9)
                        .hasCompleteness("bucket_date", lambda completeness: completeness >= 0.98)
                        .hasCompleteness("measurement_start_time", lambda completeness: completeness >= 0.9)
                        .isContainedIn("probe_cc", ["BR", "RU", "US", "DE"])
                        # .hasCompleteness("is_ip_valid", lambda completeness: completeness >= 0.9)
                        # .hasCompleteness("is_url_valid", lambda completeness: completeness >= 0.9)
                        .hasSize(lambda size: size > 0))
                        .run())
        
        df_check_result = VerificationResult.checkResultsAsDataFrame(self.spark,check_result).withColumn("bucket_date", F.lit(self.bucket_date))
        
        df_check_result.coalesce(1).write.mode("append").parquet(f's3://{args["bucket_logs"]}/validation/trusted/{datetime.datetime.now().isoformat()}')
        
        df_check_result_error = df_check_result.filter(F.col("constraint_status") != "Success")
        if len(df_check_result_error.take(1)) != 0:
            print("ERROR - DATAQUALITY")
            subset_drop = []
            flag_probe_cc = False

            collect_errors = df_check_result_error.collect()

            for row in collect_errors:
                if "id" in row.constraint:
                    subset_drop.append("id")
                if "probe_asn" in row.constraint:
                    subset_drop.append("probe_asn")
                if "probe_cc" in row.constraint:
                    subset_drop.append("probe_cc")
                if "probe_ip" in row.constraint:
                    subset_drop.append("probe_ip")
                if "test_start_time" in row.constraint:
                    subset_drop.append("test_start_time")
                if "test_name" in row.constraint:
                    subset_drop.append("test_name")
                if "bucket_date" in row.constraint:
                    subset_drop.append("bucket_date")
                if "measurement_start_time" in row.constraint:
                    subset_drop.append("measurement_start_time")
                if "is_ip_valid" in row.constraint:
                    subset_drop.append("is_ip_valid")
                if "is_url_valid" in row.constraint:
                    subset_drop.append("is_url_valid")
                if "probe_cc" in row.constraint:
                    flag_probe_cc = True

            if subset_drop:
                df = df.na.drop(subset=subset_drop)
            if flag_probe_cc:
                df = df.where(F.col("probe_cc").isin(["BR", "RU", "US", "DE"]))
        
        return df    
s3 = boto3.client('s3')
bucket_raw = args["bucket_raw"]

response = s3.list_objects_v2(
    Bucket=bucket_raw,
    Delimiter='/'
)

if 'CommonPrefixes' in response:
    for prefix in response['CommonPrefixes']:
        bucket_date = prefix['Prefix'].split('/')[0]
        print(f"Processando bucket_date: {bucket_date}")

        extract_data = ExtractData(
            spark=spark,
            file_directory=f's3://{bucket_raw}/{bucket_date}/', 
            file_type='json'
        )

        df_raw = extract_data.extract()

        transform_data = Transformation(df_raw, spark, bucket_date)
        df_ooni = transform_data.transform()
        df_ooni = df_ooni.cache()
        df_ooni_validated = transform_data.data_quality(df_ooni)
        df_ooni_validated = df_ooni_validated.cache().coalesce(1)
        
        output_directory = f's3://{args["bucket_trusted"]}/trusted'
        write_data = DataWriter()
        write_data.write_parquet(df_ooni_validated, output_directory, ['bucket_date'])

else:
    print('Nada a processar! Raw está vazia.')
