import boto3  
 
# Nome do bucket (o que você criou no S3)
BUCKET = "nome do bucket"
 
# Credenciais do AWS Academy: Learner Lab > AWS Details > AWS CLI > Show
ACCESS_KEY = "chave de acesso"       # aws_access_key_id
SECRET_KEY = "chave secreta"         # aws_secret_access_key
SESSION_TOKEN = "token de sessao"    # aws_session_token (a chave enorme)
 
# Região do Learner Lab (normalmente us-east-1)
REGIAO = "us-east-1"
# -------------------------------------
 
# Cria o cliente do S3 usando as credenciais acima
s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN,
    region_name=REGIAO
)
 
 
def enviar_para_s3(caminho_local, chave_s3):
    # caminho_local = onde o arquivo está na máquina
    # chave_s3 = "caminho" do arquivo dentro do bucket (ex: bronze/arquivo.csv)
    s3.upload_file(caminho_local, BUCKET, chave_s3)  # envia o arquivo
    print(f"Enviado para s3://{BUCKET}/{chave_s3}")