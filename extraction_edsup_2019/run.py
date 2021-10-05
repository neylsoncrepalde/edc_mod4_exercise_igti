import zipfile
import requests
from io import BytesIO
import os
import boto3

basepath = "./edsup2019"

# Cria um diretório para armazenar o conteúdo do enade
os.makedirs(basepath, exist_ok=True)

print("Extracting data...")

# Define a url e faz o download do conteúdo
url = "https://download.inep.gov.br/microdados/microdados_educacao_superior_2019.zip"
filebytes = BytesIO(requests.get(url, stream=True).content)

print("Unzip files...")
# Extrai o conteúdo do zipfile
myzip = zipfile.ZipFile(filebytes)
myzip.extractall(basepath)

print("BASE PATH....")
print(os.listdir(basepath))

# Pega a pasta "do meio" com caracteres esquisitos
pastadomeio = os.listdir(basepath)[-1]

print("ESTRUTURA DE PASTAS...")
print(basepath + '/' + pastadomeio + '/')
print(os.listdir(basepath + '/' + pastadomeio))

s3_client = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

print("Upload ALUNO to S3...")
s3_client.upload_file(
    basepath + "/" + pastadomeio + "/dados/SUP_ALUNO_2019.CSV", 
    "dl-landing-zone-539445819060", 
    "edsup2019/aluno/SUP_ALUNO_2019.CSV"
)

print("Upload DOCENTE to S3...")
s3_client.upload_file(
    basepath + "/" + pastadomeio + "/dados/SUP_DOCENTE_2019.CSV", 
    "dl-landing-zone-539445819060", 
    "edsup2019/docente/SUP_DOCENTE_2019.CSV"
)


print("Upload CURSO to S3...")
s3_client.upload_file(
    basepath + "/" + pastadomeio + "/dados/SUP_CURSO_2019.CSV", 
    "dl-landing-zone-539445819060", 
    "edsup2019/curso/SUP_CURSO_2019.CSV"
)