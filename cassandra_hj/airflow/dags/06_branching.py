from airflow.decorators import dag, task
from datetime import datetime
 
@dag(dag_id="06_branching_2", start_date=datetime(2025,9,3), catchup=False, schedule="@once")
def branching_dag():
    
    @task
    def start():
        print("Start")
    # BRANCHING TASK
    # @task.branch
    # def branch():
    #     import random
    #     choice = random.choice(['path_a','path_b'])
    #     print(f"Branching to {choice}")
    #     # print(f"XCom value: {choice}")
    #     return choice
    
    # @task
    # def path_a():
    #     print("Executing Path A")
    
    # @task
    # def path_b():
    #     print("Executing Path B")
    
    # @task
    # def join():
    #     print("Joining paths")
    
    # start_task = start()
    # branch_task = branch()
    
    # path_a_task = path_a()
    # path_b_task = path_b()
    
    # join_task = join()
    
    # start_task >> branch_task>> path_a_task >> join_task
    # branch_task >> path_b_task >> join_task

    # # SHORT CIRCUITING EXAMPLE
    # @task.short_circuit
    # def check_condition():
    #     import random
    #     condition = random.choice([True, False,False])
    #     print(f"Condition is {condition}")
    #     return condition
    # @task
    # def task_if_true():
    #     print("Condition was True, executing this task.")
    # start_task = start()
    # condition_task = check_condition()
    # true_task = task_if_true()
    # start_task >> condition_task >> true_task

    # THE SPECIALIZED OPERATORS: handle complex scenarios with minimal code.
        # 1 BranchSQLOperator
    
    from airflow.providers.common.sql.operators.sql import BranchSQLOperator
    @task
    def large_data_path():
        print("Handling large data path")
    @task
    def small_data_path():
        print("Handling small data path")

    # large_task = large_data_path()
    # small_task = small_data_path()
    branch_sql = BranchSQLOperator(
        task_id="branch_sql",
        conn_id="postgres_default",
        # sql= "SELECT COUNT(*) > 1000 FROM daily_data WHERE date = CURRENT_DATE",
        sql="SELECT TRUE",
        follow_task_ids_if_true=["large_data_path"],
        follow_task_ids_if_false=["small_data_path"]
    )
    branch_sql>>[large_data_path(),small_data_path()]
        # 2. BranchDayOfWeekOperator
    # from airflow.prividers.standard.operators.weekday import BranchDayOfWeekOperator

    # weekend_branch = BranchDayOfWeekOperator(
    #     task_id="is_weekend",
    #     week_day={5,6},
    #     follow_task_ids_if_true=["weekend_task"],
    #     follow_task_ids_if_false=["weekday_task"]
    # )
    
        # 3. BranchDatetimeOperator
    from airflow.providers.standard.operators.datetime import BranchDateTimeOperator
    from datetime import datetime, time, timedelta

    night_branch = BranchDateTimeOperator(
        task_id="is_night_time",
        target_lower = time(22,0), # 10 PM
        target_upper = time(6,0),  # 6 AM
        follow_task_ids_if_true=["night_task"],
        follow_task_ids_if_false=["day_task"],
    )


# ### 워크플로우의 분기(Branching)

# #### 1교시: 분기 소개
# **분기**는 워크플로우가 실행 조건에 따라 현명한 결정을 내리고 실행 경로를 선택하도록 돕습니다. 다음 상황에서 분기를 활용할 수 있습니다.
# * 데이터 유효성 검사
# * 환경별 처리
# * 시간 기반 의사 결정
# * 리소스 최적화

# 분기는 자원 낭비와 유연하지 않은 일방적 처리를 방지합니다. 분기가 선택되지 않은 경우, 해당 작업은 실패로 처리되지 않고 "**건너뜀**"으로 표시됩니다.

# ---

# #### @task.branch 데코레이터
# 가장 유연한 분기 방식입니다. 작업 ID를 반환하는 모든 Python 함수를 허용합니다.
# * 함수는 분기 작업 바로 다음 단계에 있는 유효한 작업 ID를 반환해야 합니다.
# * 하나의 작업 ID(문자열), 작업 ID 목록 또는 `None`(이후 모든 작업 건너뛰기)을 반환할 수 있습니다.

# 복잡하고 맞춤화된 로직, 여러 조건, 동적인 의사 결정에 이상적입니다.

# ---

# #### @task.short_circuit 데코레이터
# 불리언 값(`True`/`False`)을 사용하여 "계속 진행 또는 중단"하는 간단한 의사 결정에 사용됩니다.
# * `True` = 이후 모든 작업 계속 진행
# * `False` = 이후 모든 작업 건너뛰기

# 데이터 유효성 검사 체크포인트, 데이터 가용성 확인, 시간 기반 조건에 이상적입니다. 여러 경로 중 하나를 선택하는 대신, "진행 또는 중지"와 같은 이진 결정을 내려야 할 때 사용합니다.

# ---

# #### 특수 분기 오퍼레이터
# 특정 용도를 위해 특화된 분기 오퍼레이터가 있습니다.
# * **BranchSQLOperator**: SQL 쿼리 결과를 바탕으로 데이터베이스 기반의 결정을 내립니다.
# * **BranchDayOfWeekOperator**: 현재 요일을 기준으로 분기합니다.
# * **BranchDateTimeOperator**: 시간대(업무 시간, 유지보수 시간)에 따라 분기합니다.

# 이 모든 오퍼레이터는 `follow_task_ids_if_true`와 `follow_task_ids_if_false` 매개변수를 사용하여 분기할 경로를 지정합니다.

# ---

# #### 트리거 규칙 및 모범 사례
# 트리거 규칙은 작업이 언제 실행되어야 하는지를 결정합니다.
# * 기본 트리거 규칙인 `all_success`는 분기에서 문제를 일으킬 수 있습니다. ("skipped"은 "success"과 다르기 때문입니다.)
# * 분기 이후 작업을 병합할 때는 **`none_failed_min_one_success`** 규칙을 사용하여 최소한 하나의 분기가 성공적으로 완료된 후에 다음 작업이 실행되도록 합니다.

# **모범 사례:**
# * 분기 함수는 가볍게 유지하세요. 복잡한 로직은 이전 작업에 배치하는 것이 좋습니다.
# * 모든 분기 경로를 테스트하고, 트리거 규칙을 미리 계획하세요.
# * 건너뛰는 의존성도 잘 처리할 수 있도록 설계하세요.