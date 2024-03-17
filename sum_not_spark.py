#!/usr/bin/env python
# coding: utf-8

# In[10]:


import psycopg2
import time
import math

def sum_logic():
    # PostgreSQL 연결 정보
    postgres_host = "192.168.2.123"
    postgres_port = 5432
    postgres_db = "postgres"
    postgres_user = "postgres"
    postgres_password = ""
    postgres_table = "seminar_tb"
    new_table_name= "sum_table_ns"
    
    # PostgreSQL 연결
    conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password
    )
    
    # 커서 생성
    cur = conn.cursor()
    
    delete_table_query = "DROP TABLE IF EXISTS {table};".format(table=new_table_name)
    
    # 테이블 삭제
    cur.execute(delete_table_query)
    
    start_time=time.time()
    # SQL 쿼리
    sql_query = """
        SELECT test123_id,
               SUM(year) AS sum_year,
               SUM(month) AS sum_month,
               SUM(day) AS sum_day,
               SUM(hour) AS sum_hour,
               SUM(minute) AS sum_minute,
               SUM(second) AS sum_second
        FROM {table}
        GROUP BY test123_id;
    """.format(table=postgres_table)
    end_time = time.time()
    
    print(round(end_time-start_time,3))
    # 쿼리 실행
    cur.execute(sql_query)
    
    # 결과 가져오기
    result = cur.fetchall()
    
    # 새로운 테이블 생성
    new_table_query = """
        CREATE TABLE {table} (
            test123_id INT PRIMARY KEY,
            sum_year INT8,
            sum_month INT8,
            sum_day INT8,
            sum_hour INT8,
            sum_minute INT8,
            sum_second INT8
        );
    """.format(table=new_table_name)
    cur.execute(new_table_query)
    
    # 결과를 새로운 테이블에 저장
    for row in result:
        print(row)
        insert_query = """
            INSERT INTO {table}
            VALUES ({test123_id}, {sum_year}, {sum_month}, {sum_day}, {sum_hour}, {sum_minute}, {sum_second});
        """.format(table=new_table_name, test123_id=row[0], sum_year=row[1], sum_month=row[2], sum_day=row[3], sum_hour=row[4], sum_minute=row[5], sum_second=row[6])
        cur.execute(insert_query)
    
    # 변경사항 커밋
    conn.commit()
    
    # 연결 닫기
    cur.close()
    conn.close()

if __name__ == "__main__":
    sum_logic()
