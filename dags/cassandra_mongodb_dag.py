from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator

from check_cassandra import check_cassandra_main
from kafka_create_topic_rl import kafka_create_topic_main
from kafka_consumer_cassandra import kafka_consumer_cassandra_main

start_date = datetime(2024, 11, 19, 12, 20)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


def decide_branch():
    create_topic = kafka_create_topic_main()
    if create_topic.__contains__("Created"):
        return "topics_created"
    else:
        return "topics_already_exist"


with DAG('airflow_kafka_cassandra_mongodb', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    #create_new_topic = BranchPythonOperator(task_id='create_new_topic', python_callable=decide_branch)
    create_new_topic = PythonOperator(task_id='create_new_topic', python_callable=kafka_create_topic_main)
    
    kafka_consumer_cassandra = PythonOperator(task_id='kafka_consumer_cassandra', python_callable=kafka_consumer_cassandra_main,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=45))
    
    # kafka_producer = PythonOperator(task_id='kafka_producer', python_callable=kafka_producer_main,retries=2, retry_delay=timedelta(seconds=10),execution_timeout=timedelta(seconds=45))
    
    check_cassandra = PythonOperator(task_id='check_cassandra', python_callable=check_cassandra_main,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=45))

    
    #create_new_topic >> [topics_created, topics_already_exist]>> kafka_producer
    create_new_topic
    kafka_consumer_cassandra >> check_cassandra