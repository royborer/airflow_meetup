from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from subdags.subdag import subdag

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


schedule_interval="0 * * * *"
dag = DAG('dag_ex_2', default_args=default_args, schedule_interval=schedule_interval)


t1 = DummyOperator(
    task_id='dummy_dummy',
    dag=dag)

t2 = SubDagOperator(
    task_id='my_sub_dag',
    subdag=subdag('dag_ex_2', 'my_sub_dag', schedule_interval, default_args),
    default_args=default_args,
    dag=dag,
)

(dag >> t1 >> t2)
