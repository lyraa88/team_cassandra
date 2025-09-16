# 1. 오늘 날짜로 파일명이 저장되도록 jinja 사용
# 2. MySQLOperator를 사용한 방식으로 코드 변경
# 3. task 등은 여러분이 임의로 짭니다. - task사이에 넘겨줄 변수가 있다면 XCom를 사용하면 됩니다.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pandas as pd
import os

sql_select = "SELECT * FROM employees" 


# CSV 저장 함수
def to_df(ti):
    # 데이터 가져오기
    pulled_value = ti.xcom_pull(task_ids='get_employees_table_task')
    df = pd.DataFrame(pulled_value)
    print(df)
    return df

#  Jinja 템플릿은 sql, bash_command, 또는 명시적으로 템플릿을 지원하는 파라미터에서만 적용됨
def to_csv(**kwargs):
    # 데이터가 저장될 디렉토리와 파일 이름
    output_dir = os.path.join(os.getcwd(), 'data')
    os.makedirs(output_dir, exist_ok=True)  # 디렉토리 생성
    
    ds = kwargs['ds']
    file_path = os.path.join(output_dir, f'employee_test-{ds}.csv')

    # XCom에서 데이터 가져오기
    df = kwargs['ti'].xcom_pull(task_ids='transform_df')
    df.to_csv(file_path, index=False)
    print(f"CSV 파일이 저장되었습니다: {file_path}")


# DAG 정의
dag = DAG(
    '10_mysql_to_local_csv2',
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

get_employees_table_task = SQLExecuteQueryOperator(
    task_id='get_employees_table',
    sql=sql_select,
    conn_id='MySQL_DB',
    do_xcom_push=True, # xcom으로 데이터 전달
    dag=dag,
)

transform_df = PythonOperator(
    task_id='transform_df',
    python_callable=to_df,
    dag=dag,
)

# PythonOperator로 데이터 추출 및 저장
export_to_csv = PythonOperator(
    task_id='export_to_csv',
    python_callable=to_csv,
    dag=dag,
)

get_employees_table_task >> transform_df >> export_to_csv
