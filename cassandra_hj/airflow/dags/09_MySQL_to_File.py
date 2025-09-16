from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import os

# CSV 저장 함수
def export_mysql_to_csv(**kwargs):
    # MySQL 연결 (MySQLHook 사용)
    mysql_hook = MySqlHook(mysql_conn_id='MySQL_DB')  # MySQL Connection ID
    sql = "SELECT * FROM employees"  # 실행할 SQL 쿼리

    # 데이터 가져오기
    df = mysql_hook.get_pandas_df(sql)

    # 데이터가 저장될 디렉토리와 파일 이름
    output_dir = os.path.join(os.getcwd(), 'data')
    os.makedirs(output_dir, exist_ok=True)  # 디렉토리 생성
    file_path = os.path.join(output_dir, 'employee_test- {{ ds }}.csv')

    # CSV로 저장
    df.to_csv(file_path, index=False)
    print(f"CSV 파일이 저장되었습니다: {file_path}")

# DAG 정의
dag = DAG(
    '09_mysql_to_local_csv',
    default_args={
        'owner': 'fisa',
        'start_date': datetime(2025, 3, 15),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule=timedelta(minutes=5),
    catchup=False,
    tags=["fisaai", 'MySQLtoLocal'],
)

# PythonOperator로 데이터 추출 및 저장
mysql_to_csv_task = PythonOperator(
    task_id='mysql_to_csv_task',
    python_callable=export_mysql_to_csv,
    dag=dag,
)

mysql_to_csv_task
