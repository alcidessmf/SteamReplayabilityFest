import requests
from bs4 import BeautifulSoup
import pandas as pd
from google.cloud import bigquery
import re
import os

# Configurações do GCP
v_ambiente = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
tabela_bigQuery = 'steam-replayability-fest.Ingestao.stage-steam-replayability-fest' 
schema = [
    bigquery.SchemaField('Nome_Jogo', 'STRING'),
    bigquery.SchemaField('Preco', 'STRING'),
    bigquery.SchemaField('Classificacao', 'STRING'),   
    bigquery.SchemaField('Tipo_Classificacao', 'STRING'),
    bigquery.SchemaField('Tipo_Preco', 'STRING'),
    bigquery.SchemaField('Reducoes_Preco', 'STRING'),
    bigquery.SchemaField('Tipo_Reducao_Preco', 'STRING'),
    bigquery.SchemaField('Dt_Lancamento', 'STRING'),
    bigquery.SchemaField('Ano_Lancamento', 'STRING'),
    bigquery.SchemaField('Mes_Lancamento', 'STRING')
]
client = bigquery.Client.from_service_account_json(f'{v_ambiente}')


# Configuração de acesso do request e soup
url = "https://steamdb.info/sales/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,/;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
    "DNT": "1",
    "Sec-GPC": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Priority": "u=1"
}
response = requests.get(url, headers=headers)
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
else:
    print("Falha na requisição:", response.status_code)
    exit()


# Encontrar todas as linhas de dados
rows = soup.find_all('tr', class_='app')


# Listas para armazenar os dados
nomes = []
precos = []
classificacoes = []
tipos_classificacao = []
tipos_preco = []
reducoes_preco = []
tipos_reducao_preco = []
dts_lancamento = []
anos_lancamento = []
meses_lancamento = []

for row in rows:
    # Extrair o nome do jogo
    tag_nome = row.find('a', class_='b')
    nome = tag_nome.text.strip() if tag_nome else 'N/A'
    nomes.append(nome)
    
    # Extrair todas as colunas que têm data-sort
    extracao_coluas_data_sort = row.find_all('td', {'data-sort': True})

    # Extrair o preço do jogo
    tag_preco = next((tag for tag in extracao_coluas_data_sort if 'R$' in tag.text), None)
    if tag_preco:
        preco = tag_preco.text.replace('R$', '').strip()
        tipo_preco = 'R$'
    else:
        preco = 'N/A'
        tipo_preco = 'N/A'
    precos.append(preco)
    tipos_preco.append(tipo_preco)
    
    # Extrair a classificação
    row_text = str(row)
    regex_classificacao = r'\d+\.\d+%'
    tag_classificacao = re.findall(regex_classificacao, row_text)
    if tag_classificacao:
        classificacao = tag_classificacao[0].replace('%', '').strip()
        tipo_classificacao = '%'
    else:
        classificacao = 'N/A'
        tipo_classificacao = 'N/A'
    classificacoes.append(classificacao)
    tipos_classificacao.append(tipo_classificacao)

    # Extrair campo de redução do preço
    tag_reducao_preco = next((tag for tag in extracao_coluas_data_sort if '%' in tag.text), None)
    if tag_reducao_preco:
        reducao_preco = tag_reducao_preco.text.replace('%', '').replace('-', '').strip()
        tipo_reducao_preco = '%'
    else:
        reducao_preco = 'N/A'
        tipo_reducao_preco = 'N/A'
    reducoes_preco.append(reducao_preco)
    tipos_reducao_preco.append(tipo_reducao_preco)
    
    # Extrair a data o ano e o mês de lançamento
    tag_data_lancamento = next((tag for tag in extracao_coluas_data_sort if any(month in tag.text for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])), None)
    dt_lancamento = tag_data_lancamento.text.strip() if tag_data_lancamento else 'N/A'
    if dt_lancamento != 'N/A' and len(dt_lancamento.split()) == 2:
        mes, ano = dt_lancamento.split() 
    else:
        mes, ano = 'N/A', 'N/A'
    dts_lancamento.append(dt_lancamento)
    anos_lancamento.append(ano)
    meses_lancamento.append(mes)

df = pd.DataFrame({
    'Nome_Jogo': nomes,
    'Preco': precos,
    'Classificacao': classificacoes,
    'Tipo_Classificacao':tipos_classificacao,
    'Tipo_Preco': tipos_preco,
    'Reducoes_Preco': reducoes_preco,
    'Tipo_Reducao_Preco': tipos_reducao_preco,
    'Dt_Lancamento': dts_lancamento,
    'Ano_Lancamento': anos_lancamento,
    'Mes_Lancamento': meses_lancamento
})

# Converter dados do DF para adequar ao do BigQuery
df['Nome_Jogo'] = df['Nome_Jogo'].astype(str)
df['Preco'] = df['Preco'].astype(str)
df['Classificacao'] = df['Classificacao'].astype(str)
df['Tipo_Classificacao'] = df['Tipo_Classificacao'].astype(str)
df['Tipo_Preco'] = df['Tipo_Preco'].astype(str)
df['Reducoes_Preco'] = df['Reducoes_Preco'].astype(str)
df['Tipo_Reducao_Preco'] = df['Tipo_Reducao_Preco'].astype(str)
df['Dt_Lancamento'] = df['Dt_Lancamento'].astype(str)
df['Ano_Lancamento'] = df['Ano_Lancamento'].astype(str)
df['Mes_Lancamento'] = df['Mes_Lancamento'].astype(str)

# Conexão e ingestão no BigQuery
job_config = bigquery.LoadJobConfig(schema=schema)
job = client.load_table_from_dataframe(df, tabela_bigQuery, job_config=job_config)
job.result() 

print("Dados inseridos no BigQuery com sucesso!")