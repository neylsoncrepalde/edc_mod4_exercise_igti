from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
glue = boto3.client('glue', region_name='us-east-1',
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)

from airflow.utils.dates import days_ago


def trigger_crawler_enade2017_func():
    glue.start_crawler(Name='enade2017_crawler')


with DAG(
    'desafio_final_enade_2017',
    default_args={
        'owner': 'Neylson',
        'depends_on_past': False,
        'email': ['neylson.crepalde@a3data.com.br'],
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1,
    },
    description='Extração e processamento do ENADE 2017',
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kubernetes', 'batch', 'enem'],
) as dag:

    extracao = KubernetesPodOperator(
        namespace='airflow',
        image="539445819060.dkr.ecr.us-east-1.amazonaws.com/extraction-enade-2017:v1",
        cmds=["python", "/run.py"],
        name="extraction-enade-2017",
        task_id="extraction-enade-2017",
        image_pull_policy="Always",
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
    )


    converte_parquet = SparkKubernetesOperator(
        task_id='converte_parquet',
        namespace="airflow",
        application_file="dag_enade2017_converte_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_enade2017 = PythonOperator(
        task_id='trigger_crawler_enade2017',
        python_callable=trigger_crawler_enade2017_func,
    )


extracao >> converte_parquet >> converte_parquet_monitor >> trigger_crawler_enade2017
