from airflow.decorators import dag, task, task_group

from datetime import datetime



@dag(dag_id="12_task_group", start_date=datetime(2025,9,1),schedule="@once", tags=["fisaai"])
def task_group_dag():
    
    @task
    def t1():
        print("Task 1")
    
    
    @task
    def t2():
        print("Task 2")
    
    #  여러 태스크들을 논리적인 그룹으로 묶어 가독성을 높이고 관리하기 쉽게 만듭니다
    @task_group
    def etl_group():
        @task
        def extract():
            print("extract")
        
        @task
        def transform():
            print("Transform")
        @task
        def load():
            print("Load")
        
        extract() >> transform() >> load()

    # DAG 흐름: t1 >> (그룹화된 태스크들) >> t2
    # t1() >> etl_group()
    # etl_group() >> t2()
    t1() >> etl_group() >> t2()

task_group_dag()