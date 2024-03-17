#!/usr/bin/env python
# coding: utf-8

import psycopg2
import boto3
from io import BytesIO
from io import StringIO
import pandas.io.sql as psql
import  numpy as np
import  pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import time
import math

def minio_save_logic():
    start = time.time()
    try:
        # with psycopg2.connect(host="192.168.2.122", dbname="postgres", user="postgres", password="", port="5432") as conn:
        engine = create_engine("postgresql://postgres:@192.168.2.122:5432/postgres")
        select_cmd = "SELECT * FROM test123"

        chunk_size = 1000
        chunks = []
        offset = 0
        while True:
            query = f"{select_cmd} LIMIT {chunk_size} OFFSET {offset};"
            print(query)
            df_chunk = psql.read_sql_query(query, engine)
            if df_chunk.empty:
                break
            chunks.append(df_chunk)
            offset += chunk_size

            print(offset)

        print("end chunk")
        df = pd.concat(chunks, ignore_index=True)
        # df = psql.read_sql(select_cmd,conn)
        print(df)
        # print(colnames)
        # print(len(data))
        # print(type(data))
        # print(data[:2])
        
        minio_endpoint = "http://192.168.2.122:9000"  # MinIO 서버의 엔드포인트 주소
        minio_access_key = "admin"
        minio_secret_key = "admin"
        minio_bucket_name = "test"
        
        file_name = "uploaded-data.csv"
        
        minio_client = boto3.client(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
        )

        if not any(bucket['Name'] == minio_bucket_name for bucket in minio_client.list_buckets()['Buckets']):
            minio_client.create_bucket(Bucket=minio_bucket_name)

        # 파일이 이미 존재하는지 확인 후 생성
        existing_objects = minio_client.list_objects(Bucket=minio_bucket_name, Prefix=file_name)
        for obj in existing_objects.get('Contents', []):
            minio_client.delete_object(Bucket=minio_bucket_name, Key=obj['Key'])

        csv_data = df.to_csv(index=False).encode('utf-8')
        print(type(csv_data))
        minio_client.put_object(Bucket=minio_bucket_name, Key=file_name, Body=csv_data, ContentType="text/csv")
            
    except Exception as e:
        print(repr(e))
        print("Not Connected!.")
    end = time.time()
    print("during[" + str(round(end - start,3)) + "]")
    
if __name__ == "__main__":
    minio_save_logic()