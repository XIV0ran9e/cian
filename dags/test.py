from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from typing import NoReturn
from datetime import timedelta


# DEFAULT_ARGS = {
#     'owner' : 'Vadim Kayumov',
#     'email' : 'kayumov.vadim@gmail.com',
#     'email_on_failure' : True,
#     'email_on_retry' : False,
#     'retry' : 3,
#     'retry_delay' : timedelta(minutes=1)
# }

DEFAULT_ARGS = {
    'owner' : 'Airflow Admin',
    'email' : ' airflowadmin@example.com',
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retry' : 3,
    'retry_delay' : timedelta(minutes=1)
}

dag = DAG(
    dag_id = 'test_dag',
    schedule_interval = '0 1 * * *',
    start_date = days_ago(2),
    catchup = False,
    tags = ['mlops'],
    default_args = DEFAULT_ARGS
)


def init() -> NoReturn:
    print('Hello, World!')

task_init = PythonOperator(task_id='init', python_callable=init, dag=dag)

task_init

