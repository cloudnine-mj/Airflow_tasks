#!/usr/bin/env python
# coding: utf-8

# In[9]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
import time
import math

def sum_logic():
# Spark 세션 생성
    spark = SparkSession \
        .builder \
        .config("spark.jars", "/Users/khlee/postgresql-42.7.0.jar") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "10") \
        .appName("spark_appName") \
        .getOrCreate()
    
    # PostgreSQL 연결 정보
    postgres_url = "jdbc:postgresql://192.168.2.123:5432/postgres"
    postgres_properties = {
        "user": "postgres",
        "password": "",
        "driver": "org.postgresql.Driver"
    }
    start_time = time.time()
    # PostgreSQL 테이블 읽기
    df = spark.read \
        .jdbc(url=postgres_url, table="seminar_tb", properties=postgres_properties)

    # year 컬럼을 다 더한 값을 계산
    sum_df = df.groupBy("test123_id") \
        .agg(sum("year").alias("sum_year"), \
            sum("month").alias("sum_month"), \
            sum("day").alias("sum_day"), \
            sum("hour").alias("sum_hour"), \
            sum("minute").alias("sum_minute"), \
            sum("second").alias("sum_second")
        )
    end_time = time.time()

    print(round(end_time-start_time,3))

    # 새로운 테이블에 저장
    sum_df.write \
        .jdbc(url=postgres_url, table="sum_table", mode="overwrite", properties=postgres_properties)
    end_time = time.time()

    print(round(end_time-start_time,3))
    # Spark 세션 종료
    spark.stop()

if __name__ == "__main__":
    sum_logic()
