# Branch - 선행 작업의 결과에 따라서 다음 작업이 달라져야 할 때
# = BranchOperator를 사용해서 분기를 나눕니다.
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

# 데이터 수집 과정에서 평소와 다른 이상치가 발견됐을 때 -> 전처리 
                    # 평소와 같은 값이 수집될 때 
# 모델의 예측 결과에 대해서 메트릭 측정시 좋지 않을 때
                                        # 좋을 때 
args = {
    'owner': 'fisa',
    'start_date': datetime.now(),
}

dag = DAG(
    dag_id='06_branch_operator_1',
    default_args=args,
    schedule="@daily",
    tags=["fisaai"]
    )

# 분기를 만들 때 
first_job = EmptyOperator(
    task_id='first_job',
    dag=dag,
    )

options = ['path_A', 'path_B']

def which_path():
  '''
  return the task_id which to be executed
  '''
  if True:
    task_id = 'path_B'
    # task_id = 'path_C'
  else:
    task_id = 'path_A'
  return task_id

check_situation = BranchPythonOperator(
    task_id='check_situation',
    python_callable=which_path,
    dag=dag,
    )

first_job >> check_situation

next_job = EmptyOperator(
    task_id='next_job',
    trigger_rule='none_failed', 
    # trigger_rule='one_success', # default 값은 'all_success' 입니다
    # trigger_rule='one_failed', 
    dag=dag,
    )


for option in options:
    t = EmptyOperator(
        task_id=option,
        dag=dag,
        )
    if option == 'path_B':
        dummy_follow = EmptyOperator(
            task_id='follow_' + option,
            dag=dag,
            )
        check_situation >> t >> dummy_follow >> next_job
    else:
        check_situation >> t >> next_job

'''
trigger_rule은 
Airflow에서 태스크의 실행 조건을 정의하는 데 사용됩니다. 
기본적으로 Airflow 태스크는 모든 상위 태스크가 성공적으로 완료된 경우에만 실행됩니다. 
그러나 trigger_rule을 사용하면 태스크가 실행되는 조건을 더 세밀하게 제어할 수 있습니다.

다음은 trigger_rule의 주요 옵션들입니다:


# all_success (기본값): 모든 업스트림 태스크가 성공해야만 현재 태스크가 실행됩니다. 이 규칙은 대부분의 경우에 사용되는 가장 일반적인 설정입니다.
# all_failed: 모든 상위 태스크가 실패한 경우에만 태스크가 실행됩니다.
# all_done: 모든 업스트림 태스크가 완료(성공, 실패, 건너뜀 등) 상태가 되면 현재 태스크가 실행됩니다. 업스트림 태스크의 성공 여부와 관계없이 후속 작업을 진행해야 할 때 유용합니다.
# one_success: 업스트림 태스크 중 하나라도 성공하면 현재 태스크가 실행됩니다. 여러 태스크 중 하나만 성공해도 다음 단계로 넘어가야 할 때 사용됩니다.
# one_failed: 하나 이상의 상위 태스크가 실패한 경우에 태스크가 실행됩니다.
# none_failed: 모든 업스트림 태스크가 실패하지 않았을 때 (즉, 성공하거나 건너뛰었을 때) 현재 태스크가 실행됩니다.
# none_failed_or_skipped: 모든 업스트림 태스크가 실패하거나 건너뛰지 않았을 때 (즉, 성공했을 때) 현재 태스크가 실행됩니다. 이는 all_success와 유사하지만 건너뛴 태스크를 명시적으로 고려합니다.
# none_skipped: 상위 태스크 중 스킵된 태스크가 없는 경우에 태스크가 실행됩니다.
'''
