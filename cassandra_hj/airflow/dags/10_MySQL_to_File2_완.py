# 1. 오늘 날짜로 파일명이 저장되도록 jinja 사용
# 2. MySQLOperator를 사용한 방식으로 코드 변경
# 3. task 등은 여러분이 임의로 짭니다. - task사이에 넘겨줄 변수가 있다면 XCom를 사용하면 됩니다.
# 4. EmptyOperator를 먼저 사용해서 순서 작성하면 편함. sql -> df -> csv 순
# 필요한 모듈 import
from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os

# 필요로 하는 변수, 함수 정의
    # 데이터가 저장될 디렉토리와 파일 이름
output_dir = os.path.join(os.getcwd(), 'data')
os.makedirs(output_dir, exist_ok=True)  # 디렉토리 생성


sql_retrive_table = 'SELECT * FROM employees'

# sql query의 결과를 dataframe으로 전환
def to_df(ti):
    pulled_value = ti.xcom_pull(task_ids='extract_sql_task')
    df = pd.DataFrame(pulled_value)
    return df

# dataframe으로 전환한 결과를 csv로 저장
def to_csv(**kwargs): # **kwargs를 사용해서 airflow가 가지고 있는 모든 변수를 건네받습니다.
    print(kwargs)
    ds = kwargs['ds']
    ti = kwargs['ti']
    file_path = os.path.join(output_dir, f'employee_test-{ds}.csv')
    pulled_value = ti.xcom_pull(task_ids='transform_df_task')
    pulled_value.to_csv(file_path)

# DAG 정의
dag = DAG(
    "10_MySQL_to_File2",
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

# TASK 정의
extract_sql_task = SQLExecuteQueryOperator(
    task_id='extract_sql_task',
    sql=sql_retrive_table,
    conn_id='MySQL_DB',
    dag=dag,
    do_xcom_push=True  # Operator의 실행 결과를 XCOM으로 리턴하는 파라미터
)

transform_df_task = PythonOperator(
    task_id='transform_df_task',
    dag=dag,
    python_callable=to_df,
    # depends_on_past=True,
    owner="fisa",
    retries=3,
    retry_delay=timedelta(minutes=5),
)

load_csv_task = PythonOperator(
    task_id='load_csv_task',
    dag=dag,
    python_callable=to_csv,
    # depends_on_past=True,
    owner="fisa",
    retries=3,
    retry_delay=timedelta(minutes=5),
)

# TASK 간의 순서를 작성
extract_sql_task >> transform_df_task >> load_csv_task

