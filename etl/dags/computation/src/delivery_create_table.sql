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
STORED AS PARQUET LOCATION 's3://771030032684-delivery-full/delivery/';

MSCK REPAIR TABLE ooni_data.tb_delivery