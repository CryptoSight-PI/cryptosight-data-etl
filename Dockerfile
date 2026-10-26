FROM python:3.14.4-slim

WORKDIR /app

COPY bibliotecas.txt .

RUN pip install -r bibliotecas.txt

COPY . .

CMD ["python3", "tratamento-de-dados/pipeline.py"]