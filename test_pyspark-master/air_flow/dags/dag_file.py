from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'retries': 0
    # 'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='tz_dag',
    default_args=args,
    schedule_interval=None,
    tags=['tz']
)

producer = BashOperator(
    task_id='producer',
    bash_command='python /tmp/pycharm_project_697/test_pyspark-master/main_producer.py',
    dag=dag
)

topic_1 = BashOperator(
    task_id='topic_1',
    bash_command='python /tmp/pycharm_project_697/test_pyspark-master/t_main_info_mongo.py',
    dag=dag
)

topic_2 = BashOperator(
    task_id='topic_2',
    bash_command='python /tmp/pycharm_project_697/test_pyspark-master/t_info_dir_mongo.py',
    dag=dag
)


topic_3 = BashOperator(
    task_id='topic_3',
    bash_command='python /tmp/pycharm_project_697/test_pyspark-master/t_dir_mongo.py',
    dag=dag
)

producer >> [topic_1, topic_2, topic_3]


if __name__ == "__main__":
    dag.cli()

