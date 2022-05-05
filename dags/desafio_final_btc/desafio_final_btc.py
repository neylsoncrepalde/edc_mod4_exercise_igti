from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
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


def trigger_crawler_edsup2019_aluno_func():
    glue.start_crawler(Name='edsup2019_crawler_aluno')

def trigger_crawler_edsup2019_docente_func():
    glue.start_crawler(Name='edsup2019_crawler_docente')

def trigger_crawler_edsup2019_curso_func():
    glue.start_crawler(Name='edsup2019_crawler_curso')


with DAG(
    'desafio_final_btc_edsup_2019',
    default_args={
        'owner': 'Neylson',
        'depends_on_past': False,
        'email': ['neylson.crepalde@a3data.com.br'],
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1,
    },
    description='Extração e processamento do Censo da Ed. Superior 2019',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kubernetes', 'batch', 'Censo', 'edsup'],
) as dag:

    # extracao = KubernetesPodOperator(
    #     namespace='airflow',
    #     image="539445819060.dkr.ecr.us-east-1.amazonaws.com/extraction-edsup-2019:latest",
    #     cmds=["python", "/run.py"],
    #     name="extraction-edsup-2019",
    #     task_id="extraction-edsup-2019",
    #     image_pull_policy="Always",
    #     is_delete_operator_pod=True,
    #     in_cluster=True,
    #     get_logs=True,
    # )

    inicio = DummyOperator(task_id='inicio')

    converte_ALUNOS_parquet = SparkKubernetesOperator(
        task_id='converte_ALUNOS_parquet',
        namespace="airflow",
        application_file="dag_edsup2019_converte_ALUNOS_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_ALUNOS_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_ALUNOS_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_ALUNOS_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    converte_DOCENTE_parquet = SparkKubernetesOperator(
        task_id='converte_DOCENTE_parquet',
        namespace="airflow",
        application_file="dag_edsup2019_converte_DOCENTE_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_DOCENTE_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_DOCENTE_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_DOCENTE_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    converte_CURSO_parquet = SparkKubernetesOperator(
        task_id='converte_CURSO_parquet',
        namespace="airflow",
        application_file="dag_edsup2019_converte_CURSO_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_CURSO_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_CURSO_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_CURSO_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    trigger_crawler_edsup2019_aluno = PythonOperator(
        task_id='trigger_crawler_edsup2019_ALUNOS',
        python_callable=trigger_crawler_edsup2019_aluno_func,
    )

    trigger_crawler_edsup2019_docente = PythonOperator(
        task_id='trigger_crawler_edsup2019_DOCENTE',
        python_callable=trigger_crawler_edsup2019_docente_func,
    )

    trigger_crawler_edsup2019_curso = PythonOperator(
        task_id='trigger_crawler_edsup2019_CURSO',
        python_callable=trigger_crawler_edsup2019_curso_func
    )


inicio >> [converte_CURSO_parquet, converte_DOCENTE_parquet]
converte_CURSO_parquet >> converte_CURSO_parquet_monitor >> trigger_crawler_edsup2019_curso
converte_DOCENTE_parquet >> converte_DOCENTE_parquet_monitor >> trigger_crawler_edsup2019_docente
converte_DOCENTE_parquet_monitor >> converte_ALUNOS_parquet >> converte_ALUNOS_parquet_monitor >> trigger_crawler_edsup2019_aluno
