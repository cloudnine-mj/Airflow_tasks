# Airflow_tasks (진행중...)

* 사내 세미나 준비를 위한 Airflow + Spark + PosgreSQL + Minio 구축을 위한 리포지토리

### python 코드 import (dependency)를 위한 파일

* pip-list.txt 

### airflow task를 실행시키기 위한 dag 파일

* seminar_dag.py

### postgresql에 처음 데이터를 생성하기 위한 코드

* pg-init-data.py

### postgresql에 있는 데이터를 minio에 csv로 저장하기 위한 코드

* minio-save-data.py

### minio의 csv파일을 가져와서 formatting한 후, parsing해서 postgresql에 저장하는 코드

* spark_logic.py 

### spark를 사용하지 않고 연산처리 차이를 확인하기 위한 코드

* sum_not_spark.py 

### spark를 사용하고 연산처리 차이를 확인하기 위한 코드

* sum_spark.py  

 
