from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum # python의 datetime을 좀더 편하게 사용할 수 있게 돕는 모델
from datetime import datetime

# 1. timezone을 현재 local_tz로 적용
# 2. test1 = '내가 전달한 변수 {{ ds }}'  -> 에도 오늘 날짜가 뜨도록 변경
local_tz = pendulum.timezone("Asia/Seoul")

init_args = {
'start_date' : datetime(2025, 3, 15, 2, tzinfo=local_tz),
'catch_up' : 'FALSE'
}

test1 = '내가 전달한 변수'

# 1. dag 이름은 jinja_v3 
# 3. 매일 00시 5분마다 실행되는 fisa가 소유자인 dag입니다. 

# 1) DAG 대표하는 객체를 먼저 만들기
with DAG(
    dag_id="04_jinja_v3", # DAG의 식별자용 아이디입니다. - 보통은 파일이름과 맞춤
    schedule="5 0 * * *", #  매일 00시 5분마다 실행.
    tags=["fisaai", "my_jinja_test"],
    template_searchpath= ['/opt/airflow/data'], # 디렉토리명까지 적어주면 
    default_args=init_args
    ) as dag:


# 2. BashOperator를 사용
 
  # 2) DAG를 구성하는 태스크 만들기
  # -1. bash 커맨드로 echo hello 를 실행합니다.
  t1 = BashOperator(
      task_id="jinja_test",
      owner="fisa", # 이 작업의 오너입니다. 보통 작업을 담당하는 사람 이름을 넣습니다.
      bash_command='jinja.sh',
    #   bash_command="echo Hello {{ test }}",
      # bash_command="echo Hello",
      params={'test':test1},
      dag=dag,
  )

'''
# jinja.sh
# /dags 또는 /data 디렉터리 안에 작성합니다.
#!/bin/bash

# dag 정보와 task 정보를 Jinja template으로 불러올 수 있습니다.
echo "{{ dag }}, {{ task }}"

# dag file에서 전달해 준 test 파라미터는 다음과 같이 쓸 수 있습니다.
echo "{{ params.test }}"

# execution_date를 표현합니다.
echo "execution_date : {{ ts }}"
echo "ds: {{ds}}"

# macros를 이용하여 format과 날짜 조작이 가능합니다.
echo "the day after tommorow with no dash format"
echo "{{ macros.ds_format(macros.ds_add(ds, days=2),'%Y-%m-%d', '%Y%m%d') }}"

# for, if 예제입니다.
{% for i in range(3) %}
        echo "{{ i }} {{ ds }}"
  {% if i%2 == 0  %}
                echo "{{ ds_nodash }}"
  {% endif %}
{% endfor %}

'''
