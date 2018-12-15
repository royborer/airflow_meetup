from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


from datetime import datetime, timedelta

from airflow.models import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 07),
    #'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def print_a_loop():
    for x in range(6):
        print x


dag = DAG('dag_ex_1', default_args=default_args, schedule_interval="0 * * * *")


t1 = DummyOperator(
    task_id='dummy_dummy',
    dag=dag)

t2 = BashOperator(
    task_id='echo_string',
    bash_command='echo "such a nice string!"',
    dag=dag)

t3 = PythonOperator(
    task_id='my_python_op',
    python_callable=print_a_loop,
    dag=dag
)

(dag >> t1 >> t2 >> t3)
