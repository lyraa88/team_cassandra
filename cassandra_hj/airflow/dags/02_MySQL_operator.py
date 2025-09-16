from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Default arguments for the DAG
default_args = {
    'depends_on_past': False, # 이전에 실행되지 않은 날짜가 있더라도 동작
    'retries': 2, # 재시도 횟수
    'retry_delay': timedelta(minutes=5), # 재시도를 하는 동안 기다릴 시간 
}

# SQL query to create the 'employees' table
sql_create_table = """
    CREATE TABLE IF NOT EXISTS `employees` (
        `employeeNumber` INT(11) NOT NULL,
        `lastName` VARCHAR(50) NOT NULL,
        `firstName` VARCHAR(50) NOT NULL,
        `extension` VARCHAR(10) NOT NULL,
        `email` VARCHAR(100) NOT NULL,
        `officeCode` VARCHAR(10) NOT NULL,
        `reportsTo` INT(11) DEFAULT NULL,
        `jobTitle` VARCHAR(50) NOT NULL,
        PRIMARY KEY (`employeeNumber`)
    );
"""

# SQL query to insert data into the 'employees' table
sql_insert_data = """
    INSERT INTO `employees`(`employeeNumber`,`lastName`,`firstName`,`extension`,`email`,`officeCode`,`reportsTo`,`jobTitle`) 
    VALUES (9999,'Murphy','Diane','x5800','dmurphy@classicmodelcars.com','1',NULL,'President')
    ON DUPLICATE KEY UPDATE
        lastName=VALUES(lastName),
        firstName=VALUES(firstName),
        extension=VALUES(extension),
        email=VALUES(email),
        officeCode=VALUES(officeCode),
        reportsTo=VALUES(reportsTo),
        jobTitle=VALUES(jobTitle);
"""

# Define the DAG
dag = DAG(
    '02_mysql_employee_dag',
    default_args=default_args,
    description="Create and populate 'employees' table in MySQL database",
    schedule='@daily',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["fisaai", 'mysql', 'airflow', 'employees'],
)

# Task to create the employees table
t1 = SQLExecuteQueryOperator(
    task_id='create_employees_table',
    sql=sql_create_table,
    conn_id='MySQL_DB',
    dag=dag,
)

# Task to insert data into the employees table
t2 = SQLExecuteQueryOperator(
    task_id='insert_employees_data',
    sql=sql_insert_data,
    conn_id='MySQL_DB',
    dag=dag,
)

# Define the task dependencies
t1 >> t2
