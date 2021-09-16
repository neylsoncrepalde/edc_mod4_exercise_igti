from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
glue = boto3.client('glue', region_name='us-east-1',
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)

from airflow.utils.dates import days_ago

def trigger_crawler_inscricao_func():
        glue.start_crawler(Name='enem_anon_crawler')

def trigger_crawler_final_func():
        glue.start_crawler(Name='enem_uf_final_crawler')



with DAG(
    'enem_batch_spark_k8s',
    default_args={
        'owner': 'Neylson',
        'depends_on_past': False,
        'email': ['neylson.crepalde@a3data.com.br'],
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1,
    },
    description='submit spark-pi as sparkApplication on kubernetes',
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kubernetes', 'batch', 'enem'],
) as dag:
    converte_parquet = SparkKubernetesOperator(
        task_id='converte_parquet',
        namespace="airflow",
        application_file="enem_converte_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    anonimiza_inscricao = SparkKubernetesOperator(
        task_id='anonimiza_inscricao',
        namespace="airflow",
        application_file="enem_anonimiza_inscricao.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    anonimiza_inscricao_monitor = SparkKubernetesSensor(
        task_id='anonimiza_inscricao_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='anonimiza_inscricao')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_inscricao = PythonOperator(
        task_id='trigger_crawler_inscricao',
        python_callable=trigger_crawler_inscricao_func,
    )

    agrega_idade = SparkKubernetesOperator(
        task_id='agrega_idade',
        namespace="airflow",
        application_file="enem_agrega_idade.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agrega_idade_monitor = SparkKubernetesSensor(
        task_id='agrega_idade_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agrega_idade')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    agrega_sexo = SparkKubernetesOperator(
        task_id='agrega_sexo',
        namespace="airflow",
        application_file="enem_agrega_sexo.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agrega_sexo_monitor = SparkKubernetesSensor(
        task_id='agrega_sexo_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agrega_sexo')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    agrega_notas = SparkKubernetesOperator(
        task_id='agrega_notas',
        namespace="airflow",
        application_file="enem_agrega_notas.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agrega_notas_monitor = SparkKubernetesSensor(
        task_id='agrega_notas_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agrega_notas')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    join_final = SparkKubernetesOperator(
        task_id='join_final',
        namespace="airflow",
        application_file="enem_join_final.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    join_final_monitor = SparkKubernetesSensor(
        task_id='join_final_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='join_final')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_final = PythonOperator(
        task_id='trigger_crawler_final',
        python_callable=trigger_crawler_final_func,
    )

converte_parquet >> converte_parquet_monitor >> anonimiza_inscricao >> anonimiza_inscricao_monitor
anonimiza_inscricao_monitor >> trigger_crawler_inscricao
converte_parquet_monitor >> agrega_idade >> agrega_idade_monitor
converte_parquet_monitor >> agrega_sexo >> agrega_sexo_monitor
converte_parquet_monitor >> agrega_notas >> agrega_notas_monitor
[agrega_idade_monitor, agrega_sexo_monitor, agrega_notas_monitor] >> join_final >> join_final_monitor
join_final_monitor >> trigger_crawler_final
[agrega_idade_monitor, agrega_notas_monitor] >> agrega_sexo
[agrega_idade_monitor, agrega_notas_monitor] >> anonimiza_inscricao
