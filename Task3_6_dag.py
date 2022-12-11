from datetime import datetime
import psycopg2
import random
import os
import logging
import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

FILENAME = os.path.join(os.path.expanduser('~'), 'testfile.txt')
FNAME = os.path.join(os.path.expanduser('~'), 'task36.txt')
FNAME_T_E = os.path.join(os.path.expanduser('~'), 'task_3_6.txt')

def hello():
    print("Hello!") 

def generic_number():
    print("Вывод случайного числа при помощи использования random.random()")
    print(random.randint(1,100))  
    print(random.randint(100,1000))     

def fill_file():
    n1 = random.randint(100,1000)
    n2 = random.randint(1,100)

    try:
        with open(FNAME, 'a', encoding='utf-8') as ff:        
            ff.write(str(n1))
            ff.write(' ')
            ff.write(str(n2))
            ff.writelines('\n')
            p1 = os.path.abspath('task36.txt')
            print(p1)
            ff.close  

    except Exception as e:
        log.error(e)
        raise AirflowException(e)  

    # f = open('task36.txt', 'a')    

def write_some_file():
    n1 = random.randint(100,1000)
    try:
        with open(FILENAME, 'w', encoding='utf-8') as f:        
            f.write('test1\n') 
            f.write(str(n1))

    except Exception as e:
        log.error(e)
        raise AirflowException(e)     

def print_number_of_rows():
    line = 0
    with open(FNAME_T_E) as f:
        for _ in f:
            line += 1
    print(f'There are {line} lines in the file {FNAME_T_E}')    
    f.close

def fill_file_task_e():
    n1 = random.randint(100,1000)
    n2 = random.randint(1,100)

    try:       
        with open(FNAME_T_E, 'r', encoding='utf-8') as file: 
            lines = file.readlines()
            lin = lines[:-1]
            file.close()        

        with open(FNAME_T_E, 'w', encoding='utf-8') as file1:                                               
            file1.writelines(lin)
            file1.writelines(str(n1)+str(' ')+str(n2)+str('\n'))
            file1.close()

        with open(FNAME_T_E, 'a+', encoding='utf-8') as file2:        
            df = pd.read_table(FNAME_T_E, sep=" ", header=None)
            # print(df)
            Total0 = df[0].sum()
            Total1 = df[1].sum()
            # print(Total0)
            # print(Total1)
            # print(Total0 - Total1)
            file2.writelines(str(Total0 - Total1)+str('\n'))                 
            file2.close          

    except Exception as e:
        log.error(e)
        raise AirflowException(e)   

def read_file(): 
    try:       
        with open(FNAME_T_E, 'r', encoding='utf-8') as file: 
            text = file.read()
            print (text)
            file.close()  
    except Exception as e:
        log.error(e)
        raise AirflowException(e)                



# A DAG represents a workflow, a collection of tasks
# with DAG(dag_id="Task3_6_dag", start_date=datetime(2022, 12, 11, 15, 25), end_date=datetime(2022, 12, 11, 15, 30), schedule="*/1 * * * *", max_active_runs=1) as dag:  
with DAG(dag_id="Task3_6_dag", start_date=datetime(2022, 12, 11, 16, 0), end_date=datetime(2022, 12, 11, 16, 6), schedule="1-5 * * * *", max_active_runs=1) as dag: 
# with DAG(dag_id="Task3_6_dag", start_date=datetime(2022, 12, 11), schedule="0 0 * * *") as dag: 
    
    # Tasks are represented as operators
    bash_task = BashOperator(task_id="hello", bash_command="echo hello", do_xcom_push=False)
    python_task = PythonOperator(task_id="world", python_callable = hello, do_xcom_push=False)   
    python_task_a = PythonOperator(task_id="task_a", python_callable = generic_number, do_xcom_push=False)
    python_task_c = PythonOperator(task_id="task_c", python_callable = fill_file, do_xcom_push=False)
    # write_file_task = PythonOperator(task_id='write_some_file', python_callable=write_some_file) 
    python_task_e = PythonOperator(task_id="task_e", python_callable = fill_file_task_e, do_xcom_push=False)
    number_of_rows = PythonOperator(task_id='print_number_of_rows', python_callable=print_number_of_rows) 
    python_read_file = PythonOperator(task_id='task_read_file', python_callable=read_file) 



    # Set dependencies between tasks
    bash_task >> python_task >> python_task_a >> python_task_c >> python_task_e >> number_of_rows >> python_read_file
