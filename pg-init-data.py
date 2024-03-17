#!/usr/bin/env python
# coding: utf-8

import psycopg2
import time
import math
def pg_init_logic():
    start = time.time()
    try:
        with psycopg2.connect(host="192.168.2.122", dbname="postgres", user="postgres", password="", port="5432") as conn:
            with conn.cursor() as cur:
                seq_drop_cmd = "DROP SEQUENCE IF EXISTS public.test_seq;"
                seq_create_cmd = """
                CREATE SEQUENCE test_seq
                    INCREMENT BY 1
                    MINVALUE 1
                    MAXVALUE 9223372036854775807
                    START 1
                    CACHE 1
                    NO CYCLE;
                    """
                table_drop_cmd = "DROP TABLE IF EXISTS test123;"
                table_create_cmd = """
                CREATE TABLE public.test123 (
                	test123_id int4 NOT NULL DEFAULT nextval('test_seq'::regclass),
                	value varchar(128) NULL
                );
                """
                cur.execute(table_drop_cmd)
                cur.execute(seq_drop_cmd)
                cur.execute(seq_create_cmd)
                cur.execute(table_create_cmd)
                data_insert_cmd = "INSERT INTO test123(value) VALUES(NOW());"
                cur.execute(data_insert_cmd)
                data_duplication_cmd = "INSERT INTO test123 SELECT * FROM test123;"
                for i in range(0, 21):
                    cur.execute(data_duplication_cmd)
                print("success pg_init!!")
    except Exception as e:
        print(e)
        print("Not Connected!.")
    end = time.time()
    print("during[" + str(round(end - start,3)) + "]")

if __name__ == "__main__":
    pg_init_logic()