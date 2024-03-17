#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import boto3
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, second
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import psycopg2
import time
import math

def spark_logic():
    start = time.time()
    try:
        minio_endpoint = 'http://192.168.2.122:9000'
        minio_access_key = 'admin'
        minio_secret_key = 'admin'
        minio_bucket_name = 'test'
        csv_object_name = 'uploaded-data.csv'  # 가져올 CSV 파일의 객체 이름

        # MinIO 클라이언트 초기화
        minio_client = boto3.client('s3',
                                   endpoint_url=minio_endpoint,
                                   aws_access_key_id=minio_access_key,
                                   aws_secret_access_key=minio_secret_key)
    
        # MinIO에서 CSV 파일 가져오기
        response = minio_client.get_object(Bucket=minio_bucket_name, Key=csv_object_name)
        csv_data = response['Body'].read()
    
        # StringIO를 사용하여 CSV 데이터를 Pandas DataFrame으로 읽기
        tmp_df = pd.read_csv(StringIO(csv_data.decode('utf-8')))
    
        datas = tmp_df.values
        columns = tmp_df.columns
        
        spark = SparkSession \
            .builder \
            .config("spark.jars", "/Users/khlee/postgresql-42.7.0.jar") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.cores", "2") \
            .config("spark.sql.shuffle.partitions", "10") \
            .appName("spark_appName") \
            .getOrCreate()
            
        schema = StructType([StructField(name=col, dataType=StringType(), nullable=True) for col in columns])
        df = spark.createDataFrame(datas, schema=schema).repartition(10)
        df.cache()
    
        df = df.withColumn("value", col("value").cast("timestamp"))
    
        # 연, 월, 일, 시, 분, 초 컬럼 생성
        df = df.withColumn("year", year("value"))
        df = df.withColumn("month", month("value"))
        df = df.withColumn("day", dayofmonth("value"))
        df = df.withColumn("hour", hour("value"))
        df = df.withColumn("minute", minute("value"))
        df = df.withColumn("second", second("value"))
        df = df.drop("value")
    
        postgres_url = "jdbc:postgresql://192.168.2.123:5432/postgres"
        postgres_properties = {
            "user": "postgres",
            "password": "test",
            "driver": "org.postgresql.Driver"
        }
        df.write \
        .jdbc(url=postgres_url, table="seminar_tb", mode="overwrite", properties=postgres_properties)
        
        spark.stop()
        
    except Exception as e:
        print(f"오류 발생: {e}")
    end = time.time()
    print("during[" + str(round(end - start, 3)) + "]")
    
if __name__ == "__main__":
    spark_logic()

