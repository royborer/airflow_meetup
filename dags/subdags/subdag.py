from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_a_loop():
    for x in range(6):
        print x

def subdag(parent_dag_name, child_dag_name, schedule_interval, args):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=schedule_interval,
    )

    t1 = BashOperator(
        task_id='echo_string',
        bash_command='echo "such a nice string, from a subdag!"',
        dag=dag_subdag
    )

    t2 = PythonOperator(
        task_id='my_python_op',
        python_callable=print_a_loop,
        dag=dag_subdag
    )

    (dag_subdag >> t1 >> t2)

    return dag_subdag
