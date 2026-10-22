# CryptoSight-Data

## Fluxo de dados

O coletor roda continuamente, grava os CSVs na pasta `Csv/` e envia as coletas
para o bucket Raw (configurado por `AWS_BUCKET_RAW` ou, como compatibilidade,
`AWS_BUCKET_BRONZE`). Execute-o em um terminal:

```bash
python3 Python/data-collector.py
```

Depois que houver dados, execute o pipeline em outro terminal na raiz do
repositório:

```bash
python3 tratamento-de-dados/pipeline.py
```

O pipeline executa, em sequência:

1. Envia os CSVs locais para `raw/metrics/` ou `raw/processes/`.
2. Baixa os CSVs de métricas do prefixo `raw/metrics/` e consolida os dados.
3. Limpa os dados, grava os CSVs tratados e envia os resultados para Trusted
   (`AWS_BUCKET_TRUSTED` ou `AWS_BUCKET_SILVER`).
4. Converte os dados limpos para JSON e adiciona o ID da empresa.
5. Consulta o nome da empresa no MySQL, organiza os arquivos Gold em
   `Gold/<id>-<empresa>/` e envia-os ao bucket `AWS_BUCKET_GOLD`. O pipeline
   separa os arquivos pelo ID no nome e processa cada empresa individualmente.

Configure as credenciais AWS, os buckets e os dados MySQL no arquivo `.env`
(use `.env.example` como referência). O pipeline exige que o nome de cada CSV
de métricas comece com o ID da empresa seguido de hífen ou sublinhado.

Aqui é onde se encontra o código de leitura e captura de dados que são necessários para o funcionamento da aplicação, utilizando Python, a biblioteca psutil e a biblioteca pynvml, além de scripts de análise com R.
