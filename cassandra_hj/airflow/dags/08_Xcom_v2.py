from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 자동으로 XCom에 데이터를 Push하는 함수 (return 사용)
def push_function_auto():
    return "이것은 return을 통해 XCom에 저장된 데이터입니다!"

# XCom에서 데이터를 Pull하는 함수
def pull_function_auto(ti):
    pulled_value = ti.xcom_pull(task_ids='push_task_auto')
    print(f"XCom에서 Pull한 데이터: {pulled_value}")
    # return pulled_value + "두번째"

# DAG 정의
with DAG(
    dag_id='08_xcom_return_example',
    start_date=datetime(2025, 9, 1),
    schedule='@once',
    catchup=True,
    tags=["fisaai"]
) as dag:

    # Push 작업 (return으로 자동 Push)
    push_task_auto = PythonOperator(
        task_id='push_task_auto',
        python_callable=push_function_auto
    )

    # Pull 작업
    pull_task_auto = PythonOperator(
        task_id='pull_task_auto',
        python_callable=pull_function_auto
    )

    # 작업 순서 설정
    push_task_auto >> pull_task_auto
