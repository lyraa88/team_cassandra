from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum # python의 datetime을 좀더 편하게 사용할 수 있게 돕는 모델
from datetime import datetime, timedelta

local_tz = pendulum.timezone("Asia/Seoul")

# XCom에 데이터 Push하는 함수
def push_function(ti):
    data = "XCom으로 전달된 데이터!"
    ti.xcom_push(key='sample_key', value=data)
    ti.xcom_push(key='new_sample_key', value=[1,2,3])
    print("데이터를 Push 했습니다.")

# XCom에서 데이터 Pull하는 함수
def pull_function(ti):
    pulled_value = ti.xcom_pull(task_ids='push_task', key='sample_key')
    pulled_value2 = ti.xcom_pull(task_ids='push_task', key='new_sample_key')
    print(f"XCom에서 Pull한 데이터: {pulled_value}, {pulled_value2}")

# DAG 정의
with DAG(
    dag_id='07_xcom_push_pull_example',
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule='@once',
    catchup=False,
    tags=["fisaai"]
) as dag:

    # Push 작업
    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_function,
        owner="fisa"
    )

    # Pull 작업
    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_function,
        owner="fisa"
    )

    # 작업 순서 설정
    push_task >> pull_task
