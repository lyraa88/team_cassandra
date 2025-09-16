import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook

@dag(
    dag_id="13_MySQL_to_S3_decorator",
    start_date=datetime(2023, 8, 24),
    schedule="@once",
    catchup=False,
    tags=["fisaai", "MySQLtoS3"],
)
def mysql_to_s3_dag():

    @task
    def transfer_data_to_s3():
        # MySQL Hook을 사용하여 데이터베이스에 연결
        mysql_hook = MySqlHook(mysql_conn_id='MySQL_DB')
        
        # SQL 쿼리 실행 및 데이터 가져오기
        sql_query = "SELECT * FROM employees"
        df = mysql_hook.get_pandas_df(sql_query)
        
        # DataFrame을 CSV 문자열로 변환
        csv_string = df.to_csv(index=False, header=True)
        
        # S3 Hook을 사용하여 S3에 연결
        s3_hook = S3Hook(aws_conn_id='AWS_S3')
        
        # S3에 파일 업로드
        s3_bucket = 'woorifisa-ai'
        s3_key = 'yeonji/employee_decorator.csv'
        s3_hook.load_string(
            string_data=csv_string,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True, # 기존 파일이 있으면 덮어쓰기
            encoding='utf-8',
            )
        
        print(f"데이터가 {s3_bucket}/{s3_key}에 성공적으로 업로드되었습니다.")

    # 태스크 호출
    transfer_data_to_s3()

# DAG 등록
mysql_to_s3_dag()


# s3_bucket: 데이터가 저장될 장소
# s3_key: 이름. 스키마의 table 명과 비슷한 개념
# sql_conn_id, aws_conn_id: sql, aws(s3) connection.
# replace: S3 내부에 파일이 있다면 대체할 지에 대한 설정. sql의 if exist와 비슷한 맥락
# pd_kwargs: dataframe에 대한 설정값
