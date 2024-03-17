#!/usr/bin/env python
# coding: utf-8

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}
dag = DAG(
    'seminar_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
install_dependencies_task = BashOperator(
    task_id='install_dependencies',
    bash_command='pip install -r /Users/kanga/airflow/tmp-dir/pip-list.txt',
    dag=dag,
)
# DAG 실행 시, 필요한 스크립트를 실행하는 task 추가
pg_init_task = BashOperator(
    task_id='pg_init_data',
    bash_command='python /Users/kanga/airflow/scripts/pg-init-data.py',
    dag=dag,
    execution_timeout=timedelta(minutes=30),
)
minio_save_task = BashOperator(
    task_id='minio_save_data',
    bash_command='python /Users/kanga/airflow/scripts/minio-save-data.py',
    dag=dag,
    execution_timeout=timedelta(minutes=30),
)
spark_logic_task = BashOperator(
    task_id='spark_logic',
    bash_command='python /Users/kanga/airflow/scripts/spark_logic.py',
    dag=dag,
    execution_timeout=timedelta(minutes=30),
)
install_dependencies_task >> pg_init_task >> minio_save_task >> spark_logic_task