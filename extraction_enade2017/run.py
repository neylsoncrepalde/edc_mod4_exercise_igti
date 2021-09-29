import zipfile
import requests
from io import BytesIO
import os
import boto3

# Cria um diretório para armazenar o conteúdo do enade
os.makedirs('./enade2017', exist_ok=True)

print("Extracting data...")

# Define a url e faz o download do conteúdo
url = "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_Enade_2017_portal_2018.10.09.zip"
filebytes = BytesIO(requests.get(url).content)

print("Unzip files...")
# Extrai o conteúdo do zipfile
myzip = zipfile.ZipFile(filebytes)
myzip.extractall('./enade2017')


print("Upload to S3...")
s3_client = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
s3_client.upload_file(
    "./enade2017/3.DADOS/MICRODADOS_ENADE_2017.txt", 
    "dl-landing-zone-539445819060", 
    "enade2017/MICRODADOS_ENADE_2017.txt"
)