import boto3
from s3config import s3, bucket_bronze
from io import BytesIO, StringIO
import pandas as pd

paginator = s3.get_paginator('list_objects_v2')

page_iterator = paginator.paginate(Bucket=bucket_bronze)

for page in page_iterator:
    print(page['Contents'])
    #retorna em [{'key' : caminho}]

    for i in page['Contents']:
        print(i)
        print(i['Key'])

        key = i['Key']


df = pd.read_csv(BytesIO(s3.get_object(Bucket=bucket_bronze, Key = key)['Body'].read()) , sep = ";")
#retorna as leituras dentro dessa key / caminho

#manipula a vontade já que pegou o df
#vai juntar com o csv_bind os arquivos do bronze e deixar como um consolidado (all colected) e fica sobrescrevendo pra pegar o atual
#mas nao perde o historico já que os brutos de coleta tao indo pro bronze tambem

print(df)

