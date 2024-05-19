import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import os

# Configurações do BigQuery
v_ambiente = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
projeto = "steam-replayability-fest"
dataset = "Ingestao"
tabela_origem = "stage-steam-replayability-fest"
tabela_destino = "data-stable-steam-replayability-fest"

client = bigquery.Client.from_service_account_json(f'{v_ambiente}')
novo_schema = [
    bigquery.SchemaField('Nome_Jogo', 'STRING'),
    bigquery.SchemaField('Preco', 'FLOAT'),
    bigquery.SchemaField('Tipo_Preco', 'STRING'),
    bigquery.SchemaField('Classificacao', 'FLOAT'),   
    bigquery.SchemaField('Tipo_Classificacao', 'STRING'),
    bigquery.SchemaField('Reducoes_Preco', 'INTEGER'),
    bigquery.SchemaField('Tipo_Reducao_Preco', 'STRING'),
    bigquery.SchemaField('Dt_Lancamento', 'DATE'),
    bigquery.SchemaField('Ano_Lancamento', 'INTEGER'),
    bigquery.SchemaField('Mes_Lancamento', 'STRING')
]

# Consulta para selecionar todos os dados da tabela de origem
consulta = f"SELECT * FROM `{projeto}.{dataset}.{tabela_origem}`"
Dados_Staging = client.query(consulta).result()
df_staging = Dados_Staging.to_dataframe()


# Transformações nos dados da Staging
## Ano
df_staging['Ano_Lancamento'] = pd.to_numeric(df_staging['Ano_Lancamento'], errors='coerce')
df_staging['Ano_Lancamento'] = df_staging['Ano_Lancamento'].fillna(0).astype(int)

## Data Completa
df_staging['Dt_Lancamento'] = df_staging.apply(lambda row: datetime.strptime(f"01/{row['Mes_Lancamento']}/{row['Ano_Lancamento']}", "%d/%b/%Y") if row['Ano_Lancamento'] != 0 else datetime(1900, 1, 1), axis=1)

## Redução Preço
df_staging['Reducoes_Preco'] = df_staging['Reducoes_Preco'].astype(int)

## Preço
df_staging['Preco'] = df_staging['Preco'].str.replace(',', '.')
df_staging['Preco'] = pd.to_numeric(df_staging['Preco'], errors='coerce')

## Classificação
df_staging['Classificacao'] = pd.to_numeric(df_staging['Classificacao'], errors='coerce')

# Buscando deixar na mesma ordem do site, em conjunto com as novas colunsa adicionadas para melhorar a analise dos dados
nova_ordem_colunas = ['Nome_Jogo', 'Reducoes_Preco', 'Tipo_Reducao_Preco', 'Tipo_Preco', 'Preco', 'Classificacao', 'Tipo_Classificacao', 
                       'Dt_Lancamento', 'Ano_Lancamento', 'Mes_Lancamento']
df_staging = df_staging[nova_ordem_colunas]

# Ingestão dos dados estáveis
job_config = bigquery.LoadJobConfig(schema=novo_schema)
tabela_referencia = f"{projeto}.{dataset}.{tabela_destino}"
job = client.load_table_from_dataframe(df_staging, tabela_referencia, job_config=job_config)
job.result() 

print("Dados inseridos no BigQuery com sucesso!")